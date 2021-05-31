define easy_pool 
    set print pretty on
    set pagination off
    set logging on

    set $eio_list = &easy_io_list_var
    set $eio_offset = 0x8
    set $conn_offset = 0x28
    set $msg_offset = 0
    set $session_offset = 0
    set $eio_item = $eio_list->next
    set $eio_pool_total_size = 0

    while ($eio_item != $eio_list)
        set $eio = (easy_io_t *)((char *)$eio_item - $eio_offset)
        echo "==> eio:"
        print *$eio
         
        #eio thread num
        set $ioth_num = $eio->io_thread_pool->thread_count
        #eio thread size
        set $ioth_size = $eio->io_thread_pool->member_size
        printf "\n==> eio ioth_num :%d io thread size :%d\n", $ioth_num, $ioth_size * $ioth_num
        
        #eio pool size 
        set $eio_pool_size = 0
        set $eio_pool = $eio->pool->current
        while ($eio_pool)
            set $eio_pool_size = $eio_pool_size + ((char*)$eio_pool->end - (char *)$eio_pool)
            set $eio_pool = $eio_pool->next
        end
        printf "==> eio pool size :%d\n", $eio_pool_size

        set $io_index = 0
        set $total_server_conn_num = 0
        set $total_client_conn_num = 0
        set $session_pool_size = 0
        while ($io_index < $ioth_num)
            printf "======> io thread %d\n", $io_index
            set $ioth = (easy_io_thread_t *)((char*)$eio->io_thread_pool->data + $ioth_size * $io_index)

            set $session_list = &($ioth->session_list)
            set $session_node = $session_list->next
            while ($session_node != $session_list)
                set $session = (easy_session_t *)((char *)$session_node - $session_offset)
                set $session_pool = $session->pool->current
                while ($session_pool)
                    set $session_pool_size = $session_pool_size + ((char *)$session_pool->end - (char *)$session_pool)
                    set $session_pool = $session_pool->next
                    printf "==========> session pool size :%d",  $session_pool_size
                end
                set $session_node = $session_node->next
                echo "\n==========> session node"
                print $session_node

            end
            printf "\n========> ioth session pool size :%d\n",  $session_pool_size

            set $conn_list = &($ioth->connected_list)
            set $conn_node = $conn_list->next
            set $server_conn_num = 0
            set $client_conn_num = 0
            set $server_conn_pool_size = 0
            set $msg_pool_size = 0
            set $client_conn_pool_size = 0
            set $total_conn_pool_size = 0
            while ($conn_node != $conn_list)
                set $conn = (easy_connection_t *)((char *)$conn_node - $conn_offset)
                # server
                if ($conn->type == 0)
                    set $server_conn_num = $server_conn_num + 1
                    set $conn_pool = $conn->pool->current
                    while ($conn_pool)
                        set $server_conn_pool_size = $server_conn_pool_size + ((char*)$conn_pool->end - (char *)$conn_pool)
                        set $conn_pool = $conn_pool->next
                        printf "==========> server conn pool size :%d\n", $server_conn_pool_size
                    end

                    set $msg_list = &($conn->message_list)
                    set $msg_node = $msg_list->next
                    while ($msg_node != $msg_list)
                        set $msg = (easy_message_t *)((char *)$msg_node - $msg_offset)
                        set $msg_pool = $msg->pool->current
                        while ($msg_pool)
                            set $msg_pool_size = $msg_pool_size + ((char*)$msg_pool->end - (char *)$msg_pool)
                            set $msg_pool = $msg_pool->next
                            printf "==========> msg pool size :%d\n", $msg_pool_size
                        end
                        set $msg_node = $msg_node->next
                        printf "==========> msg node size :%d\n", $msg_pool_size
                    end
                end
                # client
                if ($conn->type == 1)
                    set $client_conn_num = $client_conn_num + 1
                    set $conn_pool = $conn->pool->current
                    while ($conn_pool)
                        set $client_conn_pool_size = $client_conn_pool_size + ((char *)$conn_pool->end - (char *)$conn_pool)
                        set $conn_pool = $conn_pool->next
                        printf "==========> client conn pool size :%d\n", $client_conn_pool_size
                    end
                end
                set $conn_node = $conn_node->next
                echo "==========> conn_node"
                print $conn_node
            end
            set $total_server_conn_num = $total_server_conn_num + $server_conn_num
            set $total_client_conn_num = $total_client_conn_num + $client_conn_num
            set $total_conn_pool_size = $server_conn_pool_size + $msg_pool_size + $client_conn_pool_size
            printf "========> ioth server conn num :%d client conn nnum :%d server conn pool size :%d msg pool size :%d client conn sie :%d total conn size %d\n", $server_conn_num, $client_conn_num, $server_conn_pool_size, $msg_pool_size, $client_conn_pool_size, $total_conn_pool_size
            set $eio_pool_size = $eio_pool_size + $total_conn_pool_size + $session_pool_size
            set $io_index = $io_index + 1
        end

        printf "====> this eio total server conn :%d total client conn :%d total pool size :%d\n", $total_server_conn_num, $total_client_conn_num, $eio_pool_size
        set $eio_pool_total_size = $eio_pool_total_size + $eio_pool_size
        set $eio_item = $eio_item->next
    end
    printf "=> all eio total pool size :%d\n", $eio_pool_total_size
end
