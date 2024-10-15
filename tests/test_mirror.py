from anystore.mirror import mirror
from anystore.store import get_store


def test_mirror(tmp_path, fixtures_path):
    source = get_store(fixtures_path)
    target = get_store(tmp_path / "1")
    assert not len([k for k in target.iterate_keys()])
    mirrored, skipped = mirror(source, target)
    assert mirrored == 6
    assert skipped == 0
    assert source.get("lorem.txt") == target.get("lorem.txt")
    assert len([k for k in target.iterate_keys()]) == len(
        [k for k in source.iterate_keys()]
    )
    mirrored, skipped = mirror(source, target)
    assert mirrored == 0
    assert skipped == 6
    mirrored, skipped = mirror(source, target, overwrite=True)
    assert mirrored == 6
    assert skipped == 0

    target = get_store(tmp_path / "2")
    mirrored, skipped = mirror(source, target, prefix="subdir")
    assert mirrored == 1
    assert skipped == 0
    assert len([k for k in target.iterate_keys()]) == 1
    assert source.get("lorem.txt") == target.get("subdir/lorem.txt")
