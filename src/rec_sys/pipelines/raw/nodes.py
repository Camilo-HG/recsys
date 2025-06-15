"""
This file defines the nodes for extraction layer pipelines
"""

import logging
import os
import zipfile
from io import BytesIO
from pathlib import Path
from typing import BinaryIO
from urllib.parse import urlparse

import requests


def check_url_is_a_zip_from_header(url: str) -> tuple[bool, str]:
    """
    Checks if a file at a given URL is likely a ZIP file based on its
    HTTP Content-Type header.

    Args:
        url (str): The URL of the file.

    Returns:
        bool: True if the file is likely a ZIP, False otherwise.
        str: A message explaining the result.
    """
    logger = logging.getLogger(__name__)

    # Check Content-Type header
    try:
        # Use a HEAD request to get headers without downloading the full content
        response = requests.head(url, allow_redirects=True, timeout=5)
        response.raise_for_status()  # Raise an HTTPError for bad responses (4xx or 5xx)

        content_type = response.headers.get("Content-Type", "").lower()
        logger.info(f"DEBUG: Content-Type for {url}: {content_type}")  # For debugging

        if (
            "application/zip" in content_type
            or "application/x-zip-compressed" in content_type
        ):
            return (
                True,
                f"Based on Content-Type header: '{content_type}', it is identified as a ZIP file.",
            )
        else:
            return (
                False,
                f"Content-Type header '{content_type}' does not indicate a ZIP file.",
            )

    except requests.exceptions.RequestException as e:
        return False, f"Could not check URL due to network error or invalid URL: {e}"
    except Exception as e:
        return False, f"An unexpected error occurred: {e}"


def download_file_from_url(
    url: str, omit_verification: bool = False
) -> tuple[BinaryIO, str]:
    """
    Downloads a ZIP file from url

    Args:
        url (str): A URL to download the ZIP file.
        omit_verification (bool): If True, omits the verification of the file extension within the URL.

    Returns:
        url_content (BinaryIO): Binary content of the ZIP file.
        file_name (str): The name of the ZIP file.
    """
    logger = logging.getLogger(__name__)

    # Parse the URL
    parsed_url = urlparse(url)

    # Get the path component of the URL
    path = Path(parsed_url.path)

    # Extract the base filename from the path
    file_name = path.name

    logger.info(f"The file name is: {file_name}")

    logger.info(f"Downloading the file from {url} ...")

    if omit_verification:
        # If verification is omitted, we assume the file is a ZIP
        is_zip = True
        message = "Verification of the file type is omitted, assuming it is a ZIP file."
        logger.info("Omitting verification of the file type based on URL extension.")
    else:
        # Check if the URL corresponds to a ZIP file

        # Check file extension from URL path
        file_extension = path.suffix.lower()

        match file_extension:
            # Determine if the file is a ZIP based on its extension
            case ".zip":
                is_zip = True
                message = f"Based on URL extension: '{file_extension}', it appears to be a ZIP file."
            case "":
                is_zip = False
                message = "The URL does not have a file extension, cannot determine if it is a ZIP file."
            case _:
                if "." in file_extension:
                    is_zip = False
                    message = f"Based on URL extension: '{file_extension}', it does not appear to be a ZIP file."
                else:
                    logger.info(
                        f"Based on URL extension: '{file_extension}', it does not appear to be a ZIP file. Verifying via Content-Type header..."
                    )
                    is_zip, message = check_url_is_a_zip_from_header(url)

    # If the file extension is .zip or if we are omitting verification, proceed with downloading
    if is_zip or omit_verification:
        logger.info(f"File verification: {message}")
        # Proceed with downloading the file
        try:
            response = requests.get(url, stream=True)
            response.raise_for_status()  # Raise an exception for HTTP errors (4xx or 5xx)

            # Save the zip file to a temporary location or directly process it in memory
            # For large files, streaming to disk is better. For this small file, in-memory is fine.
            # We'll use BytesIO to handle it in memory for simplicity here.
            url_content = BytesIO(response.content)
            logger.info("Download complete.")
            return url_content, file_name

        except requests.exceptions.RequestException as e:
            raise RuntimeError(f"Error downloading the file: {e}") from e
        except TypeError as e:
            raise ValueError(f"Invalid URL: {url}. Error: {e}") from e
    else:
        raise ValueError(
            f"The url provided {url} doesn't correspond to a ZIP file. {message}"
        )


def extract_zip_content(zip_content: BinaryIO, zip_file_name: str, extract_to_dir: str):
    """
    Extract the content from zip_content into extract_to_dir path.

    Args:
        zip_content (BinaryIO): Binary content of the ZIP file.
        zip_file_name (str): The name of the ZIP file.
        extract_to_dir (str): Path to extract ZIP fle content.
    """
    logger = logging.getLogger(__name__)

    logger.info(f"Unzipping {zip_file_name} to {extract_to_dir} ...")
    try:
        # Create the directory if it doesn't exist
        os.makedirs(extract_to_dir, exist_ok=True)

        with zipfile.ZipFile(zip_content, "r") as zip_ref:
            zip_ref.extractall(extract_to_dir)
        logger.info("Unzipping complete.")
    except zipfile.BadZipFile as e:
        raise ValueError(
            f"Error: The downloaded file '{zip_file_name}' is not a valid zip file."
        ) from e
    except Exception as e:
        raise RuntimeError(f"An error occurred during unzipping: {e}") from e


def download_and_extract_zip(
    url: str, extract_to_dir: str, omit_verification: bool = False
):
    """
    Download a ZIP file from a URL and extract its contents to a specified directory.

    Args:
        url (str): The URL of the ZIP file.
        extract_to_dir (str): The directory where the ZIP file contents will be extracted.
        omit_verification (bool): If True, skips verification of the file type based on URL extension.
    """
    url_content, file_name = download_file_from_url(url, omit_verification)
    extract_zip_content(url_content, file_name, extract_to_dir)
