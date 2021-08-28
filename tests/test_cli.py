import pytest

from yatq.worker.cli import build_parser


def test_parser_argument_handling():
    parser = build_parser()
    args = parser.parse_args("test.module.Class queue_name_1 queue_name_2".split())

    assert args.settings_module == "test.module.Class"
    assert args.queue_names == ["queue_name_1", "queue_name_2"]


def test_parser_required_queue_name():
    parser = build_parser()

    with pytest.raises(SystemExit):
        args = parser.parse_args("test.module.Class".split())
