from kedro.pipeline import Pipeline, node, pipeline

from .nodes import (
    create_model_input_table,
    preprocess_companies,
    preprocess_delivery_data,
    preprocess_movies_data,
    preprocess_shuttles,
)


def create_pipeline(**kwargs) -> Pipeline:
    return pipeline(
        [
            ### --- Examples
            node(
                func=preprocess_companies,
                inputs="companies",
                outputs="preprocessed_companies",
                name="preprocess_companies_node",
            ),
            node(
                func=preprocess_shuttles,
                inputs="shuttles",
                outputs="preprocessed_shuttles",
                name="preprocess_shuttles_node",
            ),
            node(
                func=create_model_input_table,
                inputs=["preprocessed_shuttles", "preprocessed_companies", "reviews"],
                outputs="model_input_table",
                name="create_model_input_table_node",
            ),
            ### --- Ours
            node(
                func=preprocess_movies_data,
                inputs=[
                    "movies",
                    "ratings",
                ],
                outputs=["ratings_with_titles_df_encoded", "movies_encoder"],
                name="preprocess_movies_node",
                tags=["recsys"],
            ),
            node(
                func=preprocess_delivery_data,
                inputs=[
                    "delivery",
                ],
                outputs=["delivery_df_encoded", "delivery_encoder"],
                name="preprocess_delivery_node",
                tags=["recsys"],
            ),
        ]
    )
