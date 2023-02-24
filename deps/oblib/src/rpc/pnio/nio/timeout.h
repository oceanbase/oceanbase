static bool is_epoll_handle_timeout(int64_t time_limit)
{
  return time_limit > 0 && rk_get_corse_us() > time_limit;
}

static int64_t get_epoll_handle_time_limit()
{
  return EPOLL_HANDLE_TIME_LIMIT > 0?  rk_get_corse_us() + EPOLL_HANDLE_TIME_LIMIT: -1;
}
