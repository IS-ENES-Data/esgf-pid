import uuid
from .argsutils import check_presence_of_mandatory_args

def make_handle_from_drsid_and_versionnumber(**args):
    check_presence_of_mandatory_args(args, ['drs_id','version_number','prefix'])
    suffix = make_suffix_from_drsid_and_versionnumber(drs_id=args['drs_id'], version_number=args['version_number'])
    return _suffix_to_handle(args['prefix'], suffix)

def _suffix_to_handle(prefix, suffix):
    return 'hdl:'+prefix+'/'+suffix

def concatenate_drs_and_versionnumber(drs_id, version_number):
    return drs_id+'.v'+str(version_number)

def make_suffix_from_drsid_and_versionnumber(**args):
    check_presence_of_mandatory_args(args, ['drs_id','version_number'])
    hash_basis = concatenate_drs_and_versionnumber(args['drs_id'], args['version_number'])
    return _make_uuid_from_basis(hash_basis)

def _make_uuid_from_basis(hash_basis):
    hash_basis_utf8 = hash_basis.encode('utf-8')
    ds_uuid = uuid.uuid3(uuid.NAMESPACE_URL, hash_basis_utf8) # Using uuid3, as this is easy to use also with Java
    return str(ds_uuid)
    # Which NAMESPACE (1st arg) we use does not matter, as uuids only have to be unique inside ESGF, not globally.
    # It just has to stay the same all the time! Otherwise we will create
    # different handles for the same dataset.

def make_handle_from_list_of_strings(sorted_list_of_strings, prefix, addition=None):
    concatenated = ''.join(sorted_list_of_strings)

    # We add a string to the string to make sure the result is not
    # the same for a shopping cart with only one dataset, and for
    # the dataset itself.
    if addition is not None:
        concatenated = str(addition)+concatenated
    
    suffix = _make_uuid_from_basis(concatenated)
    return _suffix_to_handle(prefix, suffix)

def make_sorted_lowercase_list_without_hdl(strings):
    # Make lowercase
    # Remove "hdl:"
    # Sort list
    newlist = []
    for string in strings:
        string = string.lower()
        string = string.replace('hdl:', '', 1)
        newlist.append(string)
    newlist.sort()
    return newlist