from honeycomb import get_option


def inform(msg):
    if get_option('verbose'):
        print(msg)
