from anystore import serialize
from anystore.mixins import BaseModel


def test_serialize():
    assert serialize.from_cloud(serialize.to_cloud("hello")) == "hello"
    assert serialize.from_cloud(serialize.to_cloud(b"hello")) == b"hello"
    assert (
        serialize.from_cloud(
            serialize.to_cloud("hello", use_pickle=False), use_pickle=False
        )
        == "hello"
    )
    assert (
        serialize.from_cloud(
            serialize.to_cloud(b"hello", use_pickle=False), use_pickle=False
        )
        == b"hello"
    )
    assert serialize.from_cloud(serialize.to_cloud("1")) == "1"
    assert serialize.from_cloud(serialize.to_cloud(b"1")) == b"1"
    assert (
        serialize.from_cloud(
            serialize.to_cloud(b"1", use_pickle=False), use_pickle=False
        )
        == 1
    )
    assert serialize.from_cloud(serialize.to_cloud(1)) == 1
    assert serialize.from_cloud(serialize.to_cloud(None)) is None
    assert isinstance(serialize.from_cloud(serialize.to_cloud(True)), bool)
    assert serialize.from_cloud(serialize.to_cloud({"a": "b"})) == {"a": "b"}

    func = lambda x: x
    func2 = serialize.from_cloud(serialize.to_cloud(func))
    assert func(1) == func2(1)

    class Model(BaseModel):
        foo: str

    model = Model(foo="bar")
    assert serialize.from_cloud(serialize.to_cloud(model)) == model
