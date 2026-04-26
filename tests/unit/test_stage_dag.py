import pathlib

from ignis.parser.event_log import parse_event_log
from ignis.parser.models import Application, Stage

_FIXTURES = pathlib.Path(__file__).parent.parent / "fixtures"


def _make_app(*stages: Stage) -> Application:
    app = Application(app_id="test", app_name="test")
    for s in stages:
        app.stages[(s.stage_id, s.stage_attempt_id)] = s
    return app


def test_parent_ids_parsed():
    app = parse_event_log(str(_FIXTURES / "skew_example.ndjson"))
    for stage in app.stages.values():
        assert isinstance(stage.parent_ids, list)


def test_parents_of_returns_correct_stages():
    s0 = Stage(stage_id=0, stage_attempt_id=0, name="root")
    s1 = Stage(stage_id=1, stage_attempt_id=0, name="child", parent_ids=[0])
    app = _make_app(s0, s1)

    assert app.parents_of(s1) == [s0]
    assert app.parents_of(s0) == []


def test_children_of_returns_correct_stages():
    s0 = Stage(stage_id=0, stage_attempt_id=0, name="root")
    s1 = Stage(stage_id=1, stage_attempt_id=0, name="child", parent_ids=[0])
    s2 = Stage(stage_id=2, stage_attempt_id=0, name="child2", parent_ids=[0])
    app = _make_app(s0, s1, s2)

    children = app.children_of(s0)
    assert sorted(s.stage_id for s in children) == [1, 2]
    assert app.children_of(s1) == []


def test_parents_of_picks_latest_attempt():
    s0_attempt0 = Stage(stage_id=0, stage_attempt_id=0, name="root-a0")
    s0_attempt1 = Stage(stage_id=0, stage_attempt_id=1, name="root-a1")
    s1 = Stage(stage_id=1, stage_attempt_id=0, name="child", parent_ids=[0])
    app = _make_app(s0_attempt0, s0_attempt1, s1)

    parents = app.parents_of(s1)
    assert len(parents) == 1
    assert parents[0].stage_attempt_id == 1


def test_children_of_picks_latest_attempt():
    s0 = Stage(stage_id=0, stage_attempt_id=0, name="root")
    s1_attempt0 = Stage(stage_id=1, stage_attempt_id=0, name="child-a0", parent_ids=[0])
    s1_attempt1 = Stage(stage_id=1, stage_attempt_id=1, name="child-a1", parent_ids=[0])
    app = _make_app(s0, s1_attempt0, s1_attempt1)

    children = app.children_of(s0)
    assert len(children) == 1
    assert children[0].stage_attempt_id == 1


def test_no_parents_no_children():
    s0 = Stage(stage_id=0, stage_attempt_id=0, name="standalone")
    app = _make_app(s0)
    assert app.parents_of(s0) == []
    assert app.children_of(s0) == []
