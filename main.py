import argparse
from scroller import Scroller
from globals import SITE_ADDRESS, IMPORT_OPTION, EXPORT_OPTION, MONGO_ADDR, DOWNLOADS_FOLDER, EXTRACTED_FOLDER


def main():
    # Define the command-line arguments
    parser = argparse.ArgumentParser(description="Download and process trade data from the CBS site.")
    parser.add_argument(
        "--files_type",
        type=str,
        choices=['import', 'export'],
        default=None,
        help="Specify the type of files to download: 'import' or 'export'. If not specified, both are downloaded."
    )
    parser.add_argument(
        "--mongo_addr",
        type=str,
        default=MONGO_ADDR,
        help="MongoDB connection address. Default is {}.".format(MONGO_ADDR)
    )
    parser.add_argument(
        "--download_folder",
        type=str,
        default=DOWNLOADS_FOLDER,
        help="Folder path to store downloaded files. Default is '{}'.".format(DOWNLOADS_FOLDER)
    )
    parser.add_argument(
        "--extracted_folder",
        type=str,
        default=EXTRACTED_FOLDER,
        help="Folder path to store extracted files. Default is '{}'.".format(EXTRACTED_FOLDER)
    )
    parser.add_argument(
        "--trade_site",
        type=str,
        default=SITE_ADDRESS,
        help="The web address of the trade data site. Default is '{}'.".format(SITE_ADDRESS)
    )

    args = parser.parse_args()

    # Create an instance of Scroller with the provided command-line arguments
    scroller = Scroller(
        mongo_addr=args.mongo_addr,
        download_folder=args.download_folder,
        extracted_folder=args.extracted_folder
    )

    # Setup MongoDB and Selenium driver
    scroller.setup()

    # Parse and process files based on the specified file type
    # if args.files_type in ['import', None]:
    #     scroller.parse_table(url=args.trade_site, files_type=IMPORT_OPTION)
    if args.files_type in ['export', None]:
        scroller.parse_table(url=args.trade_site, files_type=EXPORT_OPTION)


if __name__ == "__main__":
    main()
