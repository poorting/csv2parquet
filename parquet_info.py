#! /usr/bin/env python3
import os
import sys
import pprint
import argparse
import textwrap
import pyarrow as pa
import pyarrow.parquet as pq
import pandas as pd

program_name = os.path.basename(__file__)


###############################################################################
class ArgumentParser(argparse.ArgumentParser):

    def error(self, message):
        print('\n\033[1;33mError: {}\x1b[0m\n'.format(message))
        self.print_help(sys.stderr)
        # self.exit(2, '%s: error: %s\n' % (self.prog, message))
        self.exit(2)

    # def format_usage(self):
    #     usage = super()
    #     return "CUSTOM"+usage


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
                        Provides rudimentary information about a parquet file
                        '''),
        formatter_class=argparse.RawTextHelpFormatter, )

    parser.add_argument("filename",
                        help=textwrap.dedent('''\
                        The parquet file to inspect
                        '''),
                        action="store",
                        )

    return parser


###############################################################################
def main():

    pp = pprint.PrettyPrinter(indent=4)

    parser = parser_add_arguments()
    args = parser.parse_args()

    parquet_file = pq.ParquetFile(args.filename)

    md_dict = parquet_file.metadata.to_dict()
    md_dict.pop('row_groups')
    print(f"\nMetadata for {args.filename}:")
    for key, value in md_dict.items():
        print(f"\t{key}\t: {value}")

    print(f"\nSchema for {args.filename}:")

    schema = pa.parquet.read_schema(args.filename, memory_map=True)
    schema = pd.DataFrame(({"column": name, "pa_dtype": str(pa_dtype)} for name, pa_dtype in zip(schema.names, schema.types)))
    schema = schema.reindex(columns=["column", "pa_dtype"], fill_value=pd.NA)  # Ensures columns in case the parquet file has an empty dataframe.
    pp.pprint(schema)


###############################################################################
if __name__ == '__main__':
    # Run the main process
    main()
