import sys
import logging
import argparse


# We respect IntelliJ's PEP style rules

# Classes should always inherit object explicitly
class Greeter(object):
    # This is a ctor
    def __init__(self, name):
        # This is how you check for an empty or None string
        if not name:
            # This is how we throw exceptions
            raise ValueError("name is required")
        self.name = name

    # Private function names start with double underscores
    def __prepend_title(self):
        self.name = "Mr. " + self.name

    def greet(self):
        self.__prepend_title()
        print("Hello, there! My name is {}".format(self.name))

    # This is how we define static methods (but we rarely need them)
    @staticmethod
    def purpose():  # no self
        return "To greet people"


def parse_arguments():
    parser = argparse.ArgumentParser(description='Utility intended for documenting scripting guidelines')
    parser.add_argument('--name', metavar='name', required=False, type=str, help='name of person to greet')
    return parser.parse_args()


def main():
    # Use function docstrings whenever you think they are useful
    """This is the main function"""

    # Do not introduce module/utility logic in main()
    # Just do command line param parsing and invoke modules/functions/classes to do the work.

    # Use logging for logging messages
    logging.info("main() is running!")

    # We use argparse to process command line arguments
    args = parse_arguments()

    # This is how to retrieve command line arguments (but you won't normally need it)
    print("Received the following arguments: {}".format(sys.argv))

    # This is how we invoke static methods
    print("Creating a Greeter object whose purpose is: '{}'".format(Greeter.purpose()))

    try:
        greeter = Greeter(args.name)
        greeter.greet()
    # This is how we catch exceptions
    except ValueError as e:
        # This is how we print something to stderr
        print("Encountered an error using Greeter: {}".format(e), file=sys.stderr)
    # We have finally as well
    finally:
        # We use pass to denote an empty scope
        pass


# We always check if the script is being imported or invoked directly on the command line
if __name__ == '__main__':
    # We do not introduce logic in here. We always call main().
    main()
