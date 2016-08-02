import logging
import esgfpid.exceptions

LOGGER = logging.getLogger(__name__)
LOGGER.addHandler(logging.NullHandler())

#
# Utils for task 1:
#

def extract_file_handles_from_response_json(response_json, prefix):

    if response_json is None:
        raise esgfpid.exceptions.SolrResponseError('Response is None')

    handles = _extract_file_handles_from_facetfield_trackingid(response_json, prefix)
    return handles # raises esgfpid.exceptions.SolrResponseError

def _extract_file_handles_from_facetfield_trackingid(response_json, prefix):
    return _extract_handles_from_specified_facetfield(response_json, prefix, 'tracking_id')

#
# Utils for task 2:
#

def extract_dataset_handles_from_response_json(response_json, prefix):

    if response_json is None:
        raise esgfpid.exceptions.SolrResponseError('Response is None')

    handles = _extract_dataset_handles_from_facetfield_pid(response_json, prefix)
    return handles # raises esgfpid.exceptions.SolrResponseError

def _extract_dataset_handles_from_facetfield_pid(response_json, prefix):
    return _extract_handles_from_specified_facetfield(response_json, prefix, 'pid')

def extract_dataset_version_numbers_from_response_json(response_json):

    if response_json is None:
        raise esgfpid.exceptions.SolrResponseError('Response is None')

    handles = _extract_version_numbers_from_facetfield_version(response_json)
    return handles # raises esgfpid.exceptions.SolrResponseError

def _extract_version_numbers_from_facetfield_version(response_json):
    return _extract_strings_from_specified_facetfield(response_json, 'version')

def _extract_strings_from_specified_facetfield(response_json, fieldname):
    # Extract field
    list_with_counts = _extract_field_from_response_json(response_json, fieldname) # raises esgfpid.exceptions.SolrResponseError

    # Remove the counts
    list_without_counts = _remove_counts_from_list(list_with_counts)

    # Remove duplicates
    list_without_counts_nodup = _remove_duplicates_from_list(list_without_counts)

    return list_without_counts_nodup

#
# Used by both:
#

def _extract_handles_from_specified_facetfield(response_json, prefix, fieldname):
    # Extract field
    list_with_counts_without_prefixes = _extract_field_from_response_json(response_json, fieldname) # raises esgfpid.exceptions.SolrResponseError

    # Remove the counts
    list_without_counts_without_prefixes = _remove_counts_from_list(list_with_counts_without_prefixes)

    # Remove duplicates
    list_without_counts_without_prefixes_nodup = _remove_duplicates_from_list(list_without_counts_without_prefixes)

    # Add prefixes if not there:
    list_without_counts = _prepend_prefix_to_list_items_if_not_there(list_without_counts_without_prefixes_nodup, prefix)
    
    # Add "hdl:" if not there:
    list_with_hdl = _prepend_hdl_to_list_items_of_not_there(list_without_counts)

    return list_with_hdl

def _extract_field_from_response_json(response_json, fieldname):
    facet_fields = _extract_facet_fields(response_json) # raises esgfpid.exceptions.SolrResponseError
    try:
        list_with_counts = facet_fields[fieldname]
    except KeyError:
        raise esgfpid.exceptions.SolrResponseError('No field "%s" in response' % fieldname)
    return list_with_counts

def _prepend_prefix_to_list_items_if_not_there(list_without_prefixes, prefix):
    new_list_with_prefixes = []
    for suffix_of_handle in list_without_prefixes:
        if not isinstance(suffix_of_handle, (int, long)): # remove counts (if any were left)
            if prefix+'/' not in suffix_of_handle:
                new_list_with_prefixes.append(prefix+'/'+suffix_of_handle)
            else:
                new_list_with_prefixes.append(suffix_of_handle)
    return new_list_with_prefixes

def _prepend_hdl_to_list_items_of_not_there(list_without_hdl):
    new_list_with_hdl = []
    for handle in list_without_hdl:
        if not isinstance(handle, (int, long)): # remove counts (if any were left)
            if handle.startswith('hdl:'):
                new_list_with_hdl.append(handle)
            else:
                new_list_with_hdl.append('hdl:'+handle)
    return new_list_with_hdl

def _remove_counts_from_list(list_with_counts):
    new_list_without_counts = []
    for item in list_with_counts:
        if not isinstance(item, (int, long)):
            new_list_without_counts.append(item)
    return new_list_without_counts

def _remove_duplicates_from_list(list_with_dups):
    return list(set(list_with_dups))

def _extract_facet_fields(response_json):
    try:
        facet_fields = response_json['facet_counts']['facet_fields']
        return facet_fields
    except KeyError:
        raise esgfpid.exceptions.SolrResponseError('No "facet_counts" and/or "facet_fields" in response')

