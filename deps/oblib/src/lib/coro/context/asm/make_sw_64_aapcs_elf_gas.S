#if defined(__sw_64__)
.text
.align  2
.global make_fcontext
.type   make_fcontext, %function
make_fcontext:
    #ldih $29,0($27)
    #ldi $29,0($29)
    # shift address in $16 (allocated stack) to lower 16 byte boundary
    bic $16, 0xf,$16

    # reserve space for context-data on context-stack
    subl  $16, 0x98,$16

    # third arg of make_fcontext() == address of context-function
    # store address as a PC to jump in
    stl  $18, 0x90($16)

    # save address of finish as return-address for context-function
    # will be entered after context-function returns (LR register)
    ldi  $17, finish
    stl  $17, 0x88($16)

    stl  $16, 0x80($16)

    mov  $16, $0

    ret  $31,($26),1 //jump ($26) // return pointer to context-data ($16)

finish:
    # exit code is zero
    mov  0, $0
    # exit application
    call  _exit #ldi $27,_exit #jmp ($27)

.size   make_fcontext,.-make_fcontext
# Mark that we don't need executable stack.
.section .note.GNU-stack,"",%progbits
#endif