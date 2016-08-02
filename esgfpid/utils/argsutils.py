import esgfpid.exceptions

def check_presence_of_mandatory_args(args, mandatory_args):
    missing_args = []
    for name in mandatory_args:
        if name not in args.keys():
            missing_args.append(name)
    if len(missing_args)>0:
        raise esgfpid.exceptions.ArgumentError('Missing mandatory arguments: '+', '.join(missing_args))
    else:
        return True


def check_noneness_of_mandatory_args(args, mandatory_args):
    empty_args = []
    for name in mandatory_args:
        if args[name] is None:
            empty_args.append(name)
    if len(empty_args)>0:
        raise esgfpid.exceptions.ArgumentError('Problem: These arguments are None: '+', '.join(empty_args))
    else:
        return True

def add_missing_optional_args_with_value_none(args, optional_args):
    for name in optional_args:
        if not name in args.keys():
            args[name] = None
    return args

def find_additional_args(args, known_args_list):
    additional_args = {}
    for argname in args:
        if argname not in known_args_list:
            additional_args[argname] = args[argname]
    return additional_args