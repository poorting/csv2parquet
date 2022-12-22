#! /usr/bin/env python3
import os
import sys
import pprint
import duckdb
import argparse
import textwrap

program_name = os.path.basename(__file__)


###############################################################################
class ArgumentParser(argparse.ArgumentParser):

    def error(self, message):
        print('\n\033[1;33mError: {}\x1b[0m\n'.format(message))
        self.print_help(sys.stderr)
        # self.exit(2, '%s: error: %s\n' % (self.prog, message))
        self.exit(2)


###############################################################################
# Subroutines
# ------------------------------------------------------------------------------
def parser_add_arguments():
    """
        Parse comamnd line parameters
    """
    parser = ArgumentParser(
        prog=program_name,
        description=textwrap.dedent('''\
                        Converts a CSV file to parquet using duckdb
                        '''),
        formatter_class=argparse.RawTextHelpFormatter, )

    parser.add_argument("infile",
                        metavar='CSV-file',
                        help=textwrap.dedent('''\
                        The CSV file to convert
                        '''),
                        action="store",
                        )

    parser.add_argument("outfile",
                        metavar='Parquet-file',
                        help=textwrap.dedent('''\
                        The parquet file to write to.
                        An existing file will be overwritten!
                        '''),
                        action="store",
                        )

    return parser


###############################################################################
def main():

    pp = pprint.PrettyPrinter(indent=4)

    parser = parser_add_arguments()
    args = parser.parse_args()

    print(f"Converting CSV file '{args.infile}' to parquet file '{args.outfile}'")

    con = duckdb.connect(":memory:")
    con.execute(f"""
        PRAGMA enable_progress_bar;
        COPY (SELECT * FROM read_csv_auto('{args.infile}', delim=',', header=True, SAMPLE_SIZE=-1) )
        TO '{args.outfile}'
        (FORMAT 'PARQUET', CODEC 'ZSTD');
        """)


###############################################################################
if __name__ == '__main__':
    # Run the main process
    main()
