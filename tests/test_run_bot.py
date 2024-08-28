import pytest
from discord_bot.run_bot import parse_args

def test_parse_args():
    args = parse_args(['foo'])
    assert args.config_file == 'foo'
    assert args.command == None

    args = parse_args(['foo', 'run'])
    assert args.config_file == 'foo'
    assert args.command == 'run'

    with pytest.raises(SystemExit) as exc:
      parse_args(['foo', 'db_load'])
    assert 'SystemExit' in str(exc)