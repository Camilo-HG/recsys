"""
This is the root file for pipeline creation of 'raw' layer.
"""

from kedro.pipeline import Pipeline, node, pipeline

from .nodes import download_and_extract_zip


def create_pipeline() -> Pipeline:
    """Raw pipeline.

    Raw pipeline is intended for Extraction purposes.

    It defines a pipeline that extracts data from all internal or external
    (third-party systems/storages) sources and transforms it into parquet/delta format,
    but preserving data as immutable as possible in order to form your single source of
    truth to work from.

    Remember that the concept behind raw layer is that it contains the sourced data model(s)
    that should never be changed. These data models can be un-typed in most cases e.g. csv,
    but this will vary from case to case. Then raw data is immutable data.

    With this in mind, raw pipeline is the place where all data is extracted to the raw layer,
    preserving data format or at least, data immutable.

    Returns:
        Kedro Pipeline object
    """
    return pipeline(
        [
            node(
                func=download_and_extract_zip,
                inputs=[
                    "params:movie_lens_url",
                    "params:movie_lens_extract_to_dir",
                    "params:movie_lens_omit_verification",
                ],
                outputs=None,
                name="download_and_extract_movie_lens",
            ),
            node(
                func=download_and_extract_zip,
                inputs=[
                    "params:food_delivery_url",
                    "params:food_delivery_extract_to_dir",
                    "params:food_delivery_omit_verification",
                ],
                outputs=None,
                name="download_and_extract_food_delivery",
            ),
        ],
        tags=["raw"],
    )
