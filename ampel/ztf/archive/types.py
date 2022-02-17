from typing import Any, Literal

FilterClause = dict[str, dict[Literal["$eq", "$ne", "$gt", "$gte", "$lt", "$lte", "$in", "$nin"], Any]]