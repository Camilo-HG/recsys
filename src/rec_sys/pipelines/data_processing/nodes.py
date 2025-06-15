import pandas as pd
from sklearn.preprocessing import OneHotEncoder


def _is_true(x: pd.Series) -> pd.Series:
    return x == "t"


def _parse_percentage(x: pd.Series) -> pd.Series:
    x = x.str.replace("%", "")
    x = x.astype(float) / 100
    return x


def _parse_money(x: pd.Series) -> pd.Series:
    x = x.str.replace("$", "").str.replace(",", "")
    x = x.astype(float)
    return x


def preprocess_companies(companies: pd.DataFrame) -> pd.DataFrame:
    """Preprocesses the data for companies.

    Args:
        companies: Raw data.
    Returns:
        Preprocessed data, with `company_rating` converted to a float and
        `iata_approved` converted to boolean.
    """
    companies["iata_approved"] = _is_true(companies["iata_approved"])
    companies["company_rating"] = _parse_percentage(companies["company_rating"])
    return companies


def preprocess_shuttles(shuttles: pd.DataFrame) -> pd.DataFrame:
    """Preprocesses the data for shuttles.

    Args:
        shuttles: Raw data.
    Returns:
        Preprocessed data, with `price` converted to a float and `d_check_complete`,
        `moon_clearance_complete` converted to boolean.
    """
    shuttles["d_check_complete"] = _is_true(shuttles["d_check_complete"])
    shuttles["moon_clearance_complete"] = _is_true(shuttles["moon_clearance_complete"])
    shuttles["price"] = _parse_money(shuttles["price"])
    return shuttles


def create_model_input_table(
    shuttles: pd.DataFrame, companies: pd.DataFrame, reviews: pd.DataFrame
) -> pd.DataFrame:
    """Combines all data to create a model input table.

    Args:
        shuttles: Preprocessed data for shuttles.
        companies: Preprocessed data for companies.
        reviews: Raw data for reviews.
    Returns:
        Model input table.

    """
    rated_shuttles = shuttles.merge(reviews, left_on="id", right_on="shuttle_id")
    rated_shuttles = rated_shuttles.drop("id", axis=1)
    model_input_table = rated_shuttles.merge(
        companies, left_on="company_id", right_on="id"
    )
    model_input_table = model_input_table.dropna()
    return model_input_table


### --- Working with the data --- ###


def one_hot_encode(df: pd.DataFrame, columns: list[str]) -> pd.DataFrame:
    """
    Perform one-hot encoding on specified columns of a DataFrame.

    Args:
        df (pd.DataFrame): The DataFrame to encode.
        columns (list[str]): List of column names to encode.

    Returns:
        pd.DataFrame: DataFrame with one-hot encoded columns.
    """
    encoder = OneHotEncoder(sparse_output=False, dtype=int)
    encoded_columns = encoder.fit_transform(df[columns])

    # Create a DataFrame with the encoded columns
    encoded_df = pd.DataFrame(
        encoded_columns, columns=encoder.get_feature_names_out(columns)
    )

    # Concatenate the original DataFrame with the encoded DataFrame
    df_encoded = pd.concat([df.drop(columns=columns), encoded_df], axis=1)

    return df_encoded, encoder


def preprocess_movies_data(
    movies: pd.DataFrame, ratings: pd.DataFrame
) -> tuple[pd.DataFrame, OneHotEncoder]:
    """
    Preprocess the movies data

    Args:
        movies (pd.DataFrame): The DataFrame containing movie data.
        ratings (pd.DataFrame): The DataFrame containing movie ratings.

    Returns:
        pd.DataFrame: Preprocessed DataFrame with one-hot encoded columns.
    """
    # Rename columns for consistency and good practice according to python conventions
    ratings = ratings.rename(columns={"userId": "user_id", "movieId": "movie_id"})
    movies = movies.rename(columns={"movieId": "movie_id"})

    # Merge ratings with movies to get movie titles
    ratings_with_titles_df = pd.merge(ratings, movies, on="movie_id", how="left")

    # Format time column to datetime
    ratings_with_titles_df["timestamp"] = pd.to_datetime(
        ratings_with_titles_df["timestamp"], unit="s"
    )

    # Explode the genres column into multiple rows
    ratings_with_titles_df["genres"] = ratings_with_titles_df["genres"].str.split("|")
    ratings_with_titles_df = ratings_with_titles_df.explode("genres", ignore_index=True)

    # Perform one-hot encoding on the genres column
    ratings_with_titles_df_encoded, movies_encoder = one_hot_encode(
        ratings_with_titles_df, ["genres"]
    )

    # Gropuby the same user and movie, and aggregate the ratings by taking the max
    ratings_with_titles_df_encoded = (
        ratings_with_titles_df_encoded.groupby(
            ["user_id", "movie_id", "title", "timestamp"]
        )
        .max()
        .reset_index()
    )

    return ratings_with_titles_df_encoded, movies_encoder


def preprocess_delivery_data(
    delivery: pd.DataFrame,
) -> tuple[pd.DataFrame, OneHotEncoder]:
    """
    Preprocess the delivery data.

    Args:
        delivery (pd.DataFrame): The DataFrame containing delivery data.

    Returns:
        pd.DataFrame: Preprocessed DataFrame with one-hot encoded columns.
    """
    # Select relevant columns for delivery data
    # Columns from 0 to 15 are selected and column 9 is dropped
    delivery_df = delivery[
        delivery.columns[:9].tolist() + delivery.columns[10:16].tolist()
    ].copy()  # Select the first 15 columns, dropping the 9th column

    # Rename columns for consistency and good practice according to python conventions
    # All columns are renamed to lowercase and spaces are replaced with underscores
    delivery_df.columns = [
        col.lower()
        .replace(" ", "_")
        .replace("_(", "_")
        .replace("(", "_")
        .replace(")", "")
        .replace("perference", "preference")
        for col in delivery_df.columns
    ]

    categorical_cols = (
        delivery_df.columns[1:6].tolist() + delivery_df.columns[9:].tolist()
    )

    # Perform one-hot encoding for categorical columns using the defined function
    delivery_df_encoded, delivery_encoder = one_hot_encode(
        delivery_df, columns=categorical_cols
    )

    # Create a new column 'user_id' with the same value of the index of delivery_df_encoded
    delivery_df_encoded["user_id"] = (
        delivery_df_encoded.index + 1
    )  # user_id starts from 1

    return delivery_df_encoded, delivery_encoder
