import copy

def format_error_message(msg_list):
    max_len = _get_max_length(msg_list)
    msg_list_updated = _fill_up_strings(msg_list, max_len)
    msg_list_updated = _append_line_enclosers(msg_list_updated)
    error_message_string = ('\n'.join(msg_list_updated))
    max_len_new = _get_max_length(msg_list_updated)
    start_end_line = '*' * max_len_new
    error_message_string = start_end_line +'\n'+ error_message_string +'\n'+ start_end_line
    return error_message_string

def _get_max_length(msg_list):
    max_len = 0
    for msg in msg_list:
        if len(msg) > max_len:
            max_len = len(msg)
    return max_len

def _fill_up_strings(msg_list, max_len):
    msg_list_updated = copy.copy(msg_list)
    for i in xrange(len(msg_list)):
        current_length = len(msg_list[i])
        need_to_fill = max_len - current_length
        msg_list_updated[i] = msg_list[i] + (' '*need_to_fill)
    return msg_list_updated

def _append_line_enclosers(msg_list, encl='***'):
    msg_list_updated = copy.copy(msg_list)
    for i in xrange(len(msg_list)):
        msg_list_updated[i] = encl + ' ' + msg_list[i] + ' '+ encl
    return msg_list_updated