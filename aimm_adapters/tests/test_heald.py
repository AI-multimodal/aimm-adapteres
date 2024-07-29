from pathlib import Path

import pytest
from tiled.adapters.mapping import MapAdapter
from tiled.client import Context, from_context
from tiled.server.app import build_app

from ..heald_labview import HealdLabViewTree


@pytest.fixture(scope="module")
def context():
    here = Path(__file__).parent
    files = here / ".." / "files"
    tree = MapAdapter(
        {
            "A": HealdLabViewTree.from_directory(files),
        }
    )
    app = build_app(tree)
    with Context.from_app(app) as context:
        yield context


@pytest.mark.parametrize(
    "filename, expected_size",
    [("test_data.01", (2, 4))],
)
def test_heald_labview(filename, expected_size, context):
    client = from_context(context)
    arr = client["A"][filename].read()
    assert arr.shape == expected_size
