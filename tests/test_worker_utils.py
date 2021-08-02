import pytest

from yatq.worker.utils import import_string


def test_import_class():
    cls = import_string("yatq.worker.worker_settings.WorkerSettings")

    assert isinstance(cls, type)
    assert cls.__name__ == "WorkerSettings"


def test_import_not_a_class():
    path = "invalid import string"
    with pytest.raises(ImportError) as error:
        import_string(path)

    assert error.value.args[0] == f'"{path}" doesn\'t look like a module path'


def test_import_class_not_found():
    path = "yatq.worker.worker_settings.MissingClass"
    with pytest.raises(ImportError) as error:
        import_string(path)

    assert (
        error.value.args[0]
        == 'Module "yatq.worker.worker_settings" does not define a "MissingClass" attribute'
    )
