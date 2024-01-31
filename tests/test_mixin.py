from pathlib import Path
from anystore.mixins import BaseModel


def test_mixin(fixtures_path: Path):
    class Model(BaseModel):
        foo: str
        baz: str | None = None

    uri = "http://localhost:8000/model.json"
    model = Model.from_json_uri(uri)
    assert model.foo == "bar"
    assert model.baz is None
    assert isinstance(hash(model), int)
    assert len(set([model, model])) == 1

    uri = "http://localhost:8000/model.yaml"
    model = Model.from_yaml_uri(uri)
    assert model.foo == "bar"
    assert model.baz is None

    uri = fixtures_path / "model.json"
    model = Model.from_yaml_uri(uri)
    assert model.foo == "bar"
    assert model.baz is None

    p = fixtures_path
    assert (
        Model.from_json_uri(p / "model.json")
        == Model.from_yaml_uri(p / "model.yaml")
        == Model(foo="bar")
    )

    m = Model(foo="bar", baz="")
    assert m.baz is None
