import re
# Natural sorting
# From Stack Overflow: http://stackoverflow.com/questions/5967500/how-to-correctly-sort-a-string-with-a-number-inside
# We need this, as priorities are given as integers, but we need them
# as strings to use them as dictionary keys, 

def atoi(text):
    return int(text) if text.isdigit() else text

def natural_keys(text):
    '''
    alist.sort(key=natural_keys) sorts in human order
    http://nedbatchelder.com/blog/200712/human_sorting.html
    '''
    return [ atoi(c) for c in re.split('(\d+)', text) ]