import os
import os.path

def get_boolean(arg):
    if isinstance(arg, bool):
        return arg
    elif isinstance(arg, basestring):
        if arg.lower() in ['true']:
            return True
        elif arg.lower() in ['false']:
            return False
        else:
            raise ValueError('%s (string) cannot be parsed to a boolean.', arg)
    else:
        raise ValueError('%s (%s) cannot be parsed to a boolean.', arg, type(arg))

def ensure_directory_exists(path):
    try: 
        os.mkdir(path)
    except OSError as e:
        if 'File exists' in e.message:
            if not os.path.isdir(path):
                raise

