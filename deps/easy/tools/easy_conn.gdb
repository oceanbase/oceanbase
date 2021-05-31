define easy_conn 
    set print pretty on
    set pagination off
    set logging on

    set $eio_list = &easy_io_list_var
    set $eio_offset = 0x8
    set $conn_offset = 0x28
    set $msg_offset = 0
    set $request_offset = 0x8
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
        while ($io_index < $ioth_num)
            printf "\n======> io thread %d\n", $io_index
            set $ioth = (easy_io_thread_t *)((char*)$eio->io_thread_pool->data + $ioth_size * $io_index)

	    set $session_list = &($ioth->session_list)
            set $session_node = $session_list->next
	    if ($session_node == $session_list)
		echo "session list is empty"
		print $session_node
	    end
            while ($session_node != $session_list)
                set $session = (easy_session_t *)((char *)$session_node - $session_offset)
	    end
            set $req_list = &($ioth->request_list)
            set $request_node = $req_list->next
            if ($request_node == $req_list)
                echo "ioth request list is empty\n"
            end
            while ($request_node != $req_list)
                echo "==========> request node"
                print $request_node
                set $request = (easy_request_t*)((char *)$request_node - $request_offset)
                echo "==========> request"
                print $request
                set $request_node = $req_list->next
            end

            set $conn_list = &($ioth->connected_list)
            set $conn_node = $conn_list->next
            if ($conn_node == $conn_list)
                echo "ioth connection list is empty\n"
            end
            while ($conn_node != $conn_list)
                set $conn = (easy_connection_t *)((char *)$conn_node - $conn_offset)
                # server
                if ($conn->type == 0)
                    set $msg_list = &($conn->message_list)
                    set $msg_node = $msg_list->next
		    if ($msg_node == $msg_list)
			echo "connection msg list is empty connection:"
                    	print $conn
		    end
                    while ($msg_node != $msg_list)
                        set $msg = (easy_message_t *)((char *)$msg_node - $msg_offset)

                        set $a_list = &($msg->all_list)
                        set $a_node = $a_list->next
		    	if ($a_node == $a_list)
                    	    echo "messasge all list is empty\n"
                            print $msg
		    	end

                        printf "=======> current msg pool ref :%d ", $msg->pool.ref
			print $msg
                        set $msg_node = $msg_node->next
                    end
                end
                set $conn_node = $conn_node->next
            end
            set $io_index = $io_index + 1
        end
        set $eio_item = $eio_item->next
    end
end
