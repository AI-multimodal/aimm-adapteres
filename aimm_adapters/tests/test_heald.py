from pathlib import Path

import pytest
from tiled.adapters.mapping import MapAdapter
from tiled.client import from_tree

from ..heald_labview import HealdLabViewTree


@pytest.mark.parametrize(
    "filename, expected_size",
    [("test_data.01", (2, 4))],
)
def test_heald_labview(filename, expected_size):
    here = Path(__file__).parent  # directory containing this module
    files = here / ".." / "files"
    tree = MapAdapter({"A": HealdLabViewTree.from_directory(files)})
    client = from_tree(tree)
    arr = client["A"][filename].read()
    assert arr.shape == expected_size
