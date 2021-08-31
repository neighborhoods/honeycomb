from honeycomb import get_option


def inform(msg):
    """
    Prints a string to the console, but only if the 'verbose' option is True.
    Allows for user-informing messages to be disabled in non-interactive
    contexts by setting the 'verbose' option to False.

    Args:
        msg (str): The message to conditionally print.
    """
    if get_option('verbose'):
        print(msg)
