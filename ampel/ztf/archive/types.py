from typing import Annotated, Any, Literal

from pydantic import BeforeValidator

FilterTerm = Annotated[
    dict[Literal["$eq", "$ne", "$gt", "$gte", "$lt", "$lte", "$in", "$nin"], Any],
    BeforeValidator(lambda v: v if isinstance(v, dict) else {"$eq": v}),
]

FilterClause = dict[str, FilterTerm]
