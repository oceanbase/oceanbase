ob_set_subtarget(oblib_rpc common
  ob_lock_wait_node.cpp
  ob_request.cpp
  ob_rpc_request_operator.cpp
  ob_sql_request_operator.cpp
)

ob_set_subtarget(oblib_rpc frame
  frame/ob_net_easy.cpp
  frame/ob_req_deliver.cpp
  frame/ob_req_handler.cpp
  frame/ob_req_processor.cpp
  frame/ob_req_qhandler.cpp
  frame/ob_req_queue_thread.cpp
  frame/ob_req_translator.cpp
  frame/ob_req_transport.cpp
  frame/ob_sql_processor.cpp
)

ob_set_subtarget(oblib_rpc obmysql
  obmysql/ob_2_0_protocol_processor.cpp
  obmysql/ob_2_0_protocol_utils.cpp
  obmysql/ob_easy_sql_request_operator.cpp
  obmysql/ob_mysql_compress_protocol_processor.cpp
  obmysql/ob_mysql_field.cpp
  obmysql/ob_mysql_handler.cpp
  obmysql/ob_mysql_packet.cpp
  obmysql/ob_mysql_protocol_processor.cpp
  obmysql/ob_mysql_request_utils.cpp
  obmysql/ob_mysql_row.cpp
  obmysql/ob_mysql_translator.cpp
  obmysql/ob_mysql_util.cpp
  obmysql/ob_poc_sql_request_operator.cpp
  obmysql/ob_sql_nio.cpp
  obmysql/ob_sql_nio_server.cpp
  obmysql/ob_sql_sock_handler.cpp
  obmysql/ob_sql_sock_processor.cpp
  obmysql/ob_sql_sock_session.cpp
  obmysql/ob_virtual_cs_protocol_processor.cpp
  obmysql/obp20_extra_info.cpp
  obmysql/ob_packet_record.cpp
)

ob_set_subtarget(oblib_rpc obmysql_packet
  obmysql/packet/ompk_change_user.cpp
  obmysql/packet/ompk_eof.cpp
  obmysql/packet/ompk_error.cpp
  obmysql/packet/ompk_field.cpp
  obmysql/packet/ompk_handshake.cpp
  obmysql/packet/ompk_handshake_response.cpp
  obmysql/packet/ompk_ok.cpp
  obmysql/packet/ompk_piece.cpp
  obmysql/packet/ompk_prepare.cpp
  obmysql/packet/ompk_prexecute.cpp
  obmysql/packet/ompk_resheader.cpp
  obmysql/packet/ompk_row.cpp
  obmysql/packet/ompk_ssl_request.cpp
)

ob_set_subtarget(oblib_rpc obrpc
  obrpc/ob_easy_rpc_request_operator.cpp
  obrpc/ob_net_keepalive.cpp
  obrpc/ob_listener.cpp
  obrpc/ob_net_client.cpp
  obrpc/ob_poc_rpc_proxy.cpp
  obrpc/ob_poc_rpc_request_operator.cpp
  obrpc/ob_poc_rpc_server.cpp
  obrpc/ob_rpc_compress_protocol_processor.cpp
  obrpc/ob_rpc_compress_struct.cpp
  obrpc/ob_rpc_endec.cpp
  obrpc/ob_rpc_handler.cpp
  obrpc/ob_rpc_mem_pool.cpp
  obrpc/ob_rpc_net_handler.cpp
  obrpc/ob_rpc_packet.cpp
  obrpc/ob_rpc_processor_base.cpp
  obrpc/ob_rpc_protocol_processor.cpp
  obrpc/ob_rpc_proxy.cpp
  obrpc/ob_rpc_proxy.ipp
  obrpc/ob_rpc_request.cpp
  obrpc/ob_rpc_result_code.cpp
  obrpc/ob_rpc_session_handler.cpp
  obrpc/ob_rpc_stat.cpp
  obrpc/ob_rpc_stream_cond.cpp
  obrpc/ob_rpc_time.cpp
  obrpc/ob_rpc_translator.cpp
  obrpc/ob_virtual_rpc_protocol_processor.cpp
)

ob_lib_add_target(oblib_rpc)

