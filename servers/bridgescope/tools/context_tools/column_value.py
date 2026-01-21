import numpy as np
from typing import List, Dict, Any

from mcp_constants import response_type
from tools.utils import get_db_adapter, get_semantic_model, format_response
from mcp_context import mcp


@mcp.tool()
async def search_relative_column_values(
    column_2_value: Dict[str, Any],
) -> response_type:
    """
    For each specified 'table.column', find the most semantically similar top-5 values to the given target value.

    /****IMPORTANCE****/
    Before generating the query, please make sure to understand which candidate values exist in the relevant column first.
    Otherwise, you should call this tools to query.

    Parameters:
        column_2_value (Dict[str, Any]):
            A dictionary where keys are in the format 'table.column' and values are target values
            used for semantic matching.


    Returns:
        Dict[str, List[Any]]:
            A dictionary mapping each column to a list of top semantically similar values.

    Example:
        Input:
            {
                "products.name": "wireless headphones",
                "categories.description": "audio devices"
            }

        Output:
            {
                "name": ["wireless headphones", "bluetooth earphones", "noise-canceling headset",
                         "earbuds", "audio headphones"],
                "description": ["audio devices", "sound equipment", "speaker systems",
                                "headphones", "amplifiers"]
            }
    """

    limit = 5
    max_distinct_values = 50

    # Handle empty input case
    if not column_2_value:
        raise RuntimeError("No column-value map provided.")

    db_adapter = get_db_adapter()
    semantic_model = get_semantic_model()

    result = {}

    # Process each column and its corresponding target value
    for full_column, target_value in column_2_value.items():
        # Validate the format of the column name
        if "." not in full_column:
            result[full_column] = [
                "Invalid column format. Expected 'table.column'."
            ]
            continue

        # Split into table and column names
        table, column = full_column.split(".", 1)

        # Step 1: Query all distinct values from the specified column
        query = f"SELECT DISTINCT {column} FROM {table} LIMIT {max_distinct_values}"
        values = await db_adapter.execute_query(query)

        # Extract values assuming rows are dictionaries
        all_values = [str(value[0]) for value in values]

        # Skip if no valid data found
        if not all_values:
            result[column] = []
            continue

        # Convert all values to strings for semantic encoding
        target_value = str(target_value)

        # Step 2: Encode all texts and the target text into vectors
        embeddings = semantic_model.encode(all_values)
        target_embedding = semantic_model.encode([target_value])[0]

        # Step 3: Compute cosine similarity between each value and the target
        similarities = np.dot(embeddings, target_embedding) / (
            np.linalg.norm(embeddings, axis=1) * np.linalg.norm(target_embedding)
        )

        # Step 4: Get indices of top-k most similar values
        top_indices = np.argsort(similarities)[::-1][
            :limit
        ]  # Descending order, take top-k
        top_values = [all_values[i] for i in top_indices]

        # Store result for this column
        result[column] = top_values

    return format_response(result)
