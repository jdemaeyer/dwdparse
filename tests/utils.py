from typing import Any


def is_subset(
    subset_dict: dict[str, Any],
    full_dict: dict[str, Any],
) -> bool:
    return subset_dict == {k: full_dict[k] for k in subset_dict}
