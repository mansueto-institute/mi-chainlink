import argparse
import re
import time

import duckdb
import numpy as np
import pandas as pd
import sparse_dot_topn as ct
from sklearn.feature_extraction.text import TfidfVectorizer


def superfast_tfidf(entity_list: pd.DataFrame) -> list:
    """
    returns sorted list of top matched names
    """

    # matching
    t0 = time.time()
    entity_list = entity_list[~pd.isna(entity_list["entity"])].reset_index(drop=True)
    company_names = entity_list["entity"]
    id_vector = entity_list["name_id"]
    t1 = time.time()
    vectorizer = TfidfVectorizer(min_df=1, analyzer=ngrams)
    tf_idf_matrix = vectorizer.fit_transform(company_names)
    t2 = time.time()
    matches = ct.sp_matmul_topn(
        tf_idf_matrix, tf_idf_matrix.transpose(), 50, 0.8, sort=True, n_threads=-1
    )
    t3 = time.time()
    matches_df = get_matches_df(
        sparse_matrix=matches, name_vector=company_names, id_vector=id_vector
    )
    t4 = time.time()
    matches_df = clean_matches(matches_df)
    t5 = time.time()

    print(f"Time to preprocess: {t1 - t0:.2f} seconds")
    print(f"Time to vectorize: {t2 - t1:.2f} seconds")
    print(f"Time to calculate matches: {t3 - t2:.2f} seconds")
    print(f"Time to get matches: {t4 - t3:.2f} seconds")
    print(f"Time to clean matches: {t5 - t4:.2f} seconds")

    return matches_df


def get_matches_df(
    sparse_matrix: pd.DataFrame, name_vector: list, id_vector: list, top=None
) -> pd.DataFrame:
    """
    create a matches dataframe given matrix of ngrams
    references
        sparse_matrix - matrix from vectorized comparison calculations
        name_vector - list of names to compare
        id_vector - id of distinct name from entities list
    """
    non_zeros = sparse_matrix.nonzero()

    sparserows = non_zeros[0]
    sparsecols = non_zeros[1]

    nr_matches = top if top else sparsecols.size

    entity_a = np.empty([nr_matches], dtype=object)
    entity_b = np.empty([nr_matches], dtype=object)
    similarity = np.zeros(nr_matches)
    id_a = np.empty([nr_matches], dtype=object)
    id_b = np.empty([nr_matches], dtype=object)

    for index in range(0, nr_matches):
        entity_a[index] = name_vector[sparserows[index]]
        entity_b[index] = name_vector[sparsecols[index]]
        similarity[index] = sparse_matrix.data[index]
        id_a[index] = id_vector[sparserows[index]]
        id_b[index] = id_vector[sparsecols[index]]

    data = {
        "entity_a": entity_a,
        "entity_b": entity_b,
        "similarity": similarity,
        "id_a": id_a,
        "id_b": id_b,
    }
    return pd.DataFrame(data)


def clean_matches(matches_df: pd.DataFrame) -> pd.DataFrame:
    """
    remove self matches and duplicates in match dataframe

    Returns: pd.DataFrame
    """

    # create copy to make adjustments
    matches_df = matches_df.copy()

    # remove self matches
    matches_df = matches_df[matches_df["id_a"] != matches_df["id_b"]]

    # remove duplicate matches where (A, B) and (B, A) are considered the same
    matches_df["sorted_pair"] = matches_df.apply(
        lambda row: tuple(sorted([row["id_a"], row["id_b"]])), axis=1
    )

    duplicates = matches_df.duplicated(subset="sorted_pair")
    matches_df = matches_df[~duplicates].reset_index(drop=True)
    matches_df = matches_df.drop(columns=["sorted_pair"])
    matches_df = matches_df.sort_values(by="similarity", ascending=False).reset_index(
        drop=True
    )

    return matches_df


# words we will replace in ngram
# replace with blanks
blank_words = {
    "LL",
    "LLC",
    "LP",
    "CORP",
    "CO",
    "INC",
    "LTD",
    "CORPORATION",
    "INCORPORATED",
    "PROFESSIONALS",
    "ASSOCIATION",
    "COMPANY",
}

# replace with shortened versions
ngram_adj = {
    frozenset(
        {
            "DEVELOPMENT",
            "DEVELOPMENTS",
            "DVLPMNT",
            "DEVLPMNT",
            "DEVELOPMEN",
            "DEVELOPMNT",
        }
    ): "DEV",
    frozenset({"ESTATE", "ESTATES", "ESATE", "ESTAT"}): "EST",
    frozenset({"HOUSING", "HOUSNG", "HOUSIN", "HOUISING", "HOUISNG"}): "HSNG",
    frozenset(
        {
            "MANAGEMENT",
            "MANAGEMEN",
            "MANAGMENT",
            "MANGAMENT",
            "MANGAEMENT",
            "MANAG",
            "MGMNT",
            "MNGMT",
        }
    ): "MGMT",
    frozenset(
        {
            "PROPERTY",
            "PROPERTIES",
            "PROPRETY",
            "PROPRETIES",
            "PROPERT",
            "PROPERTI",
            "PROPERTIE",
            "PROPS",
        }
    ): "PROP",
    frozenset(
        {"REALTY", "REALTIES", "RELATY", "RELATIES", "REALT", "REALTEIS", "RE", "REL"}
    ): "RLTY",
}

# Flatten ngram_adj for easier replacement
flat_ngram_adj = {
    word: replacement
    for synonyms, replacement in ngram_adj.items()
    for word in synonyms
}


def adjust_and_replace(string: str) -> str:
    """
    replace specified words with blanks and other words with their corresponding values for ngrams
    """

    # remove punctuation
    string = re.sub(r"[,-./]", r"", string)

    # split the string into words
    parts = string.split()

    # replace words based on blank_words and flat_ngram_adj using list comprehension
    adjusted_string = "".join(
        [
            "" if part in blank_words else flat_ngram_adj.get(part, part)
            for part in parts
        ]
    )

    return adjusted_string.strip()


def ngrams(string: str, n: int = 3) -> list:
    """
    split string into substrings of length n, return list of substrings
    """
    pre_processing = adjust_and_replace(string)
    ngrams = zip(*[pre_processing[i:] for i in range(n)])
    return ["".join(ngram) for ngram in ngrams]


def database_query(db_path: str, limit=None) -> pd.DataFrame:
    """
    queries entities for comparison
    """

    # start connection with woc db
    with duckdb.connect(db_path) as conn:
        entity_query = """
        SELECT entity, name_id
        FROM entity.name
        """

        # retreive entity list (all unique names in parcel, llc and corp data
        entity_list = conn.execute(entity_query).df()

        # randomized sample for limit
        if limit is not None:
            entity_list = entity_list.sample(n=limit, random_state=12345)

    return entity_list


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Process some integers.")
    parser.add_argument("file_path", type=str, help="Path to DuckDB file")
    parser.add_argument("--limit", type=int, help="Limit for entity query")
    parser.add_argument("--n", type=int, help="n-gram length for TF-IDF")
    parser.add_argument("-t", action="store_true", help="flag for testing function")

    args = parser.parse_args()

    limit = args.limit
    output_filename = "links.parquet"

    if args.t:
        if not args.limit:
            limit = 100000
        output_filename = "links_test_with.parquet"

    start_time = time.time()

    # format the time to display hours and minutes
    start_time_struct = time.localtime(start_time)
    formatted_date = time.strftime("%d-%m-%Y", start_time_struct)
    formatted_time = time.strftime("%H:%M", start_time_struct)

    print(f"Process started on {formatted_date} at {formatted_time}")

    # retrieve entity list, print length of dataframe
    entity_list = database_query(args.file_path, limit)
    print(f"Query retrieved {len(entity_list)} rows")

    matches_df = superfast_tfidf(entity_list)

    print("Fuzzy Matching done")

    # save to db
