ob_set_subtarget(ob_election common
  ob_election.cpp
  ob_election_async_log.cpp
  ob_election_base.cpp
  ob_election_event_history.cpp
  ob_election_group.cpp
  ob_election_group_id.cpp
  ob_election_group_info.cpp
  ob_election_group_mgr.cpp
  ob_election_group_cache.cpp
  ob_election_group_msg_pool.cpp
  ob_election_group_priority.cpp
  ob_election_info.cpp
  ob_election_mem_stat.cpp
  ob_election_mgr.cpp
  ob_election_msg.cpp
  ob_election_msg_pool.cpp
  ob_election_part_array_buf.cpp
  ob_election_priority.cpp
  ob_election_rpc.cpp
  ob_election_timer.cpp
  ob_election_gc_thread.cpp
)

ob_server_add_pchs(election
  ob_election_group_id.h
)

ob_server_add_target(ob_election)
