import re
from pathlib import Path

import duckdb
import numpy as np
import polars as pl
import sparse_dot_topn as ct
from sklearn.feature_extraction.text import TfidfVectorizer


def superfast_tfidf(entity_list: pl.DataFrame, id_col: str = "name_id", entity_col: str = "entity") -> list:
    """
    returns sorted list of top matched names
    """

    # matching

    entity_list = entity_list.filter(~pl.col(entity_col).is_null())
    company_names = entity_list.select(entity_col).to_series()
    if len(company_names) < 2:
        matches_df = pl.DataFrame(data={"entity_a": [], "entity_b": [], "similarity": [], "id_a": [], "id_b": []})
        return matches_df
    vectorizer = TfidfVectorizer(min_df=1, analyzer=ngrams)
    tf_idf_matrix = vectorizer.fit_transform(company_names.to_numpy())
    matches = ct.sp_matmul_topn(tf_idf_matrix, tf_idf_matrix.transpose(), 50, 0.8, sort=True, n_threads=-1)
    matches_df = get_matches_df(
        sparse_matrix=matches, name_vector=company_names.to_numpy(), entity_list=entity_list, id_col=id_col
    )
    matches_df = clean_matches(matches_df)

    return matches_df


def get_matches_df(
    sparse_matrix: np.ndarray, name_vector: list, entity_list: pl.DataFrame, id_col: str, top: None = None
) -> pl.DataFrame:
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
    # id_a = np.empty([nr_matches], dtype=np.uint64)
    # id_b = np.empty([nr_matches], dtype=np.uint64)

    for index in range(0, nr_matches):
        entity_a[index] = name_vector[sparserows[index]]
        entity_b[index] = name_vector[sparsecols[index]]
        similarity[index] = sparse_matrix.data[index]
        # id_a[index] = id_vector[int(sparserows[index])]
        # id_b[index] = id_vector[int(sparsecols[index])]

    # print(id_a)
    # print(id_b)
    # print(type(id_a))

    data = {
        "entity_a": entity_a,
        "entity_b": entity_b,
        "similarity": similarity,
        # "id_a": id_a,
        # "id_b": id_b,
    }
    df = (
        pl.DataFrame(data)
        .join(entity_list, left_on="entity_a", right_on="entity", how="left")
        .rename({id_col: "id_a"})
        .join(entity_list, left_on="entity_b", right_on="entity", how="left")
        .rename({id_col: "id_b"})
    )
    return df


def clean_matches(matches_df: pl.DataFrame) -> pl.DataFrame:
    """
    remove self matches and duplicates in match dataframe

    Returns: pl.DataFrame
    """

    # create copy to make adjustments
    # matches_df = matches_df.copy()
    # remove self matches, duplicates and sort
    matches_df = (
        matches_df.filter(pl.col("id_a") != pl.col("id_b"))
        .with_columns(pl.concat_list(pl.col("id_a", "id_b")).list.sort().alias("sorted_id_pairs"))
        .unique("sorted_id_pairs")
        .drop("sorted_id_pairs")
        .sort("similarity", descending=True)
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
    frozenset({
        "DEVELOPMENT",
        "DEVELOPMENTS",
        "DVLPMNT",
        "DEVLPMNT",
        "DEVELOPMEN",
        "DEVELOPMNT",
    }): "DEV",
    frozenset({"ESTATE", "ESTATES", "ESATE", "ESTAT"}): "EST",
    frozenset({"HOUSING", "HOUSNG", "HOUSIN", "HOUISING", "HOUISNG"}): "HSNG",
    frozenset({
        "MANAGEMENT",
        "MANAGEMEN",
        "MANAGMENT",
        "MANGAMENT",
        "MANGAEMENT",
        "MANAG",
        "MGMNT",
        "MNGMT",
    }): "MGMT",
    frozenset({
        "PROPERTY",
        "PROPERTIES",
        "PROPRETY",
        "PROPRETIES",
        "PROPERT",
        "PROPERTI",
        "PROPERTIE",
        "PROPS",
    }): "PROP",
    frozenset({"REALTY", "REALTIES", "RELATY", "RELATIES", "REALT", "REALTEIS", "RE", "REL"}): "RLTY",
}

# Flatten ngram_adj for easier replacement
flat_ngram_adj = {word: replacement for synonyms, replacement in ngram_adj.items() for word in synonyms}


def adjust_and_replace(string: str) -> str:
    """
    replace specified words with blanks and other words with their corresponding values for ngrams
    """

    # remove punctuation
    string = re.sub(r"[,-./]", r"", string)

    # split the string into words
    parts = string.split()

    # replace words based on blank_words and flat_ngram_adj using list comprehension
    adjusted_string = "".join(["" if part in blank_words else flat_ngram_adj.get(part, part) for part in parts])

    return adjusted_string.strip()


def ngrams(string: str, n: int = 3) -> list:
    """
    split string into substrings of length n, return list of substrings
    """
    pre_processing = adjust_and_replace(string)
    ngrams = zip(*[pre_processing[i:] for i in range(n)])
    return ["".join(ngram) for ngram in ngrams]


def database_query(db_path: str | Path, table_name: str | None = None, limit: int | None = None) -> pl.DataFrame:
    """
    queries entities for comparison
    """
    if table_name is None:
        table_name = "entity.name"
        id_col = "name_id"
    else:
        id_col = table_name.split(".")[1] + "_id"

    # start connection with woc db
    with duckdb.connect(db_path) as conn:
        entity_query = f"""
        SELECT entity, {id_col}
        FROM {table_name}
        """

        # retreive entity list (all unique names in parcel, llc and corp data
        entity_list = conn.execute(entity_query).pl()

        # randomized sample for limit
        if limit is not None:
            entity_list = entity_list.sample(n=limit)

    return entity_list
