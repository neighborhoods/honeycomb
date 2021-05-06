import river as rv

_options = {
    'verbose': True
}


def set_option(opt, val):
    valid_options = _options.keys()
    if opt not in valid_options:
        raise ValueError('\'{}\' is not a valid honeycomb option .')
    else:
        _options[opt] = val
        if opt == 'verbose':
            rv.set_option('verbose', val)


def get_option(opt):
    return _options.get(opt)
