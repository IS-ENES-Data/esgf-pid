
def replace_date_with_string(json_object, key='message_timestamp', replacement='anydate'):
    if key in json_object:
        json_object[key] = replacement

def is_json_same(expected, received):
    '''
    This function compares two JSON objects on equality, i.e.:
     - Do they have the same keys?
     - Do the shared keys have the same value?
    '''
    expected_keys = expected.keys()
    expected_keyscopy = expected_keys[:]
    received_keys = received.keys()
    for key in expected_keyscopy:
        if key not in received_keys:
            return False
        elif expected[key] != received[key]:
            return False
        else:
            expected_keys.remove(key)
            received_keys.remove(key)
    if len(received_keys) > 0:
        return False

    return True

def compare_json_return_errormessage(expected, received, full=True):
    '''
    Basically the same as "is_json_same()", but it does not return
    on the first found error, and collects verbose information about
    errors on the way.

    :param full: Optional. Boolean. If True, appends both full objects
                 to the error message. Defaults to True.
    :return: Error message as string if an error was found.
             Otherwise, returns None.
    '''
    error_msg = ''
    error_occurred = False

    expected_keys = expected.keys()
    expected_keyscopy = expected_keys[:]
    received_keys = received.keys()
    for key in expected_keyscopy:
        if key not in received_keys:
            #error_msg += ('\nExpected key "'+key+'" not in received keys: '+str(received_keys))
            error_msg += ('\nExpected key "%s" not in received keys: %s' % (key, received_keys))
            error_occurred = True
        elif expected[key] != received[key]:
            #error_msg += ('\nReceived for key "'+str(key)+'":\n'+str(received[key])+'\nnot same as expected\n'+str(expected[key]))
            error_msg += ('\nReceived for key "%s":\n\t%s (%s)\nnot same as expected\n\t%s (%s)' % (key, received[key], type(received[key]), expected[key], type(expected[key])))
            error_occurred = True
            expected_keys.remove(key)
            received_keys.remove(key)
        else:
            expected_keys.remove(key)
            received_keys.remove(key)
    if len(received_keys) > 0:
        #error_msg += ('\nThese keys were received, but not expected: '+str(received_keys))
        error_msg += ('\nThese keys were received, but not expected: %s' % received_keys)
        error_occurred = True

    if error_occurred:
        if full:
            #error_msg += '\nExpected:\n'+str(expected)+'\nReceived:\n'+str(received)
            error_msg += ('\nExpected:\n\t%s\nReceived:\n\t%s' % (expected, received))
        return error_msg
    else:
        return None

#def compare_json(expected, received):
#    errormsg = compare_json_return_errormessage(expected, received)
#    if errormsg is None:
#        return True
#    else:
#        return False


def is_json_in_jsonlist(json_object, json_list):
    for item in json_list:
        is_same = compare_json(item, json_object)
        if is_same:
            return True
    return False

def remove_date_simple(jsonobject):
    try:
        jsonobject['publication_date'] = 'anydate'
    except KeyError:
        for message in jsonobject:
            message['publication_date'] = 'anydate'
    return jsonobject

def remove_date_recursive(jsonobject):
    return replace_value(jsonobject,'publication_date','anydate')

def replace_value(jsonobject, key_to_replace, new_value):
    if type(jsonobject) == dict:
        if jsonobject == {}:
            return jsonobject
        else:
            try:
                jsonobject[key_to_replace] = new_value
            except KeyError:
                pass
            for key in jsonobject:
                replace_value(jsonobject[key], key_to_replace, new_value)
            return jsonobject

def remove_comment(jsonobject):
    return remove_pair(jsonobject, 'comment')

def remove_pair(jsonobject, key_to_remove):
    if type(jsonobject) == dict:
        if jsonobject == {}:
            return jsonobject
        else:
            try:
                del jsonobject[key_to_remove]
            except KeyError:
                pass
            for key in jsonobject:
                remove_pair(jsonobject[key], key_to_remove)
            return jsonobject