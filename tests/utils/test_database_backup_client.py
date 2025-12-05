import json
from sqlalchemy import text

from discord_bot.utils.database_backup_client import DatabaseBackupClient
from discord_bot.database import MarkovChannel, MarkovRelation, Playlist

from tests.helpers import fake_engine, mock_session  #pylint:disable=unused-import


def test_database_backup_client_init(fake_engine, mocker):  #pylint:disable=redefined-outer-name
    '''Test DatabaseBackupClient initialization'''
    logger = mocker.Mock()
    client = DatabaseBackupClient(fake_engine, logger)
    assert client.db_engine == fake_engine
    assert client.logger == logger
    assert client.CHUNK_SIZE == 1000


def test_create_backup_empty_database(fake_engine, mocker):  #pylint:disable=redefined-outer-name
    '''Test backup creation with empty database'''
    logger = mocker.Mock()
    client = DatabaseBackupClient(fake_engine, logger)

    backup_file = client.create_backup()

    # Verify file was created
    assert backup_file.exists()
    assert backup_file.name.startswith('db_backup_')
    assert backup_file.name.endswith('.json')

    # Verify JSON structure
    with open(backup_file, 'r', encoding='utf-8') as f:
        data = json.load(f)

    # Should have all tables from BASE.metadata
    assert isinstance(data, dict)
    assert 'markov_channel' in data
    assert 'markov_relation' in data
    assert 'playlist' in data

    # All tables should be empty arrays
    for _, rows in data.items():
        assert isinstance(rows, list)
        assert len(rows) == 0

    # Cleanup
    backup_file.unlink()


def test_create_backup_with_data(fake_engine, mocker):  #pylint:disable=redefined-outer-name
    '''Test backup creation with actual data'''
    logger = mocker.Mock()

    # Add test data
    with mock_session(fake_engine) as session:
        # Add markov channels
        channel1 = MarkovChannel(channel_id='123', server_id='456', last_message_id='789')
        channel2 = MarkovChannel(channel_id='111', server_id='222', last_message_id='333')
        session.add(channel1)
        session.add(channel2)

        # Add markov relations
        relation1 = MarkovRelation(channel_id=1, leader_word='hello', follower_word='world')
        relation2 = MarkovRelation(channel_id=1, leader_word='world', follower_word='test')
        session.add(relation1)
        session.add(relation2)

        session.commit()

    client = DatabaseBackupClient(fake_engine, logger)
    backup_file = client.create_backup()

    # Verify file was created
    assert backup_file.exists()

    # Verify JSON structure and data
    with open(backup_file, 'r', encoding='utf-8') as f:
        data = json.load(f)

    # Check markov_channel data
    assert len(data['markov_channel']) == 2
    assert data['markov_channel'][0]['channel_id'] == '123'
    assert data['markov_channel'][0]['server_id'] == '456'
    assert data['markov_channel'][1]['channel_id'] == '111'

    # Check markov_relation data
    assert len(data['markov_relation']) == 2
    assert data['markov_relation'][0]['leader_word'] == 'hello'
    assert data['markov_relation'][0]['follower_word'] == 'world'

    # Cleanup
    backup_file.unlink()


def test_create_backup_streaming_large_table(fake_engine, mocker):  #pylint:disable=redefined-outer-name
    '''Test that backup streams data in chunks for large tables'''
    logger = mocker.Mock()

    # Add more rows than CHUNK_SIZE to test streaming
    with mock_session(fake_engine) as session:
        for i in range(2500):  # More than 2x CHUNK_SIZE
            relation = MarkovRelation(
                channel_id=1,
                leader_word=f'word_{i}',
                follower_word=f'next_{i}'
            )
            session.add(relation)
        session.commit()

    client = DatabaseBackupClient(fake_engine, logger)
    backup_file = client.create_backup()

    # Verify all data was backed up
    with open(backup_file, 'r', encoding='utf-8') as f:
        data = json.load(f)

    assert len(data['markov_relation']) == 2500

    # Verify data integrity
    assert data['markov_relation'][0]['leader_word'] == 'word_0'
    assert data['markov_relation'][1000]['leader_word'] == 'word_1000'
    assert data['markov_relation'][2499]['leader_word'] == 'word_2499'

    # Cleanup
    backup_file.unlink()


def test_create_backup_only_includes_base_tables(fake_engine, mocker):  #pylint:disable=redefined-outer-name
    '''Test that backup only includes tables from BASE.metadata'''
    logger = mocker.Mock()

    # Create a table not in BASE.metadata
    with fake_engine.connect() as connection:
        connection.execute(text('CREATE TABLE custom_table (id INTEGER PRIMARY KEY, data TEXT)'))
        connection.execute(text("INSERT INTO custom_table VALUES (1, 'test')"))
        connection.commit()

    client = DatabaseBackupClient(fake_engine, logger)
    backup_file = client.create_backup()

    with open(backup_file, 'r', encoding='utf-8') as f:
        data = json.load(f)

    # custom_table should NOT be in the backup
    assert 'custom_table' not in data

    # But BASE tables should be there
    assert 'markov_channel' in data
    assert 'playlist' in data

    # Cleanup
    backup_file.unlink()


def test_create_backup_handles_special_types(fake_engine, mocker):  #pylint:disable=redefined-outer-name
    '''Test that backup handles dates and special types correctly'''
    from datetime import datetime  #pylint:disable=import-outside-toplevel
    logger = mocker.Mock()

    # Add data with dates/times
    with mock_session(fake_engine) as session:
        playlist = Playlist(
            server_id='123',
            name='test_playlist',
            is_history=False,
            created_at=datetime(2025, 1, 1, 12, 0, 0)
        )
        session.add(playlist)
        session.commit()

    client = DatabaseBackupClient(fake_engine, logger)
    backup_file = client.create_backup()

    with open(backup_file, 'r', encoding='utf-8') as f:
        data = json.load(f)

    # Check that datetime was serialized to string
    assert len(data['playlist']) == 1
    assert data['playlist'][0]['name'] == 'test_playlist'
    # Datetime should be converted to string by default=str
    assert isinstance(data['playlist'][0]['created_at'], str)
    assert '2025-01-01' in data['playlist'][0]['created_at']

    # Cleanup
    backup_file.unlink()


def test_create_backup_logging(fake_engine, mocker):  #pylint:disable=redefined-outer-name
    '''Test that backup logs appropriate messages'''
    logger = mocker.Mock()

    # Add some test data
    with mock_session(fake_engine) as session:
        channel = MarkovChannel(channel_id='123', server_id='456')
        session.add(channel)
        session.commit()

    client = DatabaseBackupClient(fake_engine, logger)
    backup_file = client.create_backup()

    # Check that debug logging was called for table backup
    debug_calls = [call.args[0] for call in logger.debug.call_args_list]
    assert any('Backing up table:' in call for call in debug_calls)

    # Check that info logging was called for completion
    info_calls = [call.args[0] for call in logger.info.call_args_list]
    assert any('Created backup file:' in call for call in info_calls)

    # Cleanup
    backup_file.unlink()


def test_create_backup_file_size(fake_engine, mocker):  #pylint:disable=redefined-outer-name
    '''Test that backup file size is logged correctly'''
    logger = mocker.Mock()

    client = DatabaseBackupClient(fake_engine, logger)
    backup_file = client.create_backup()

    # Verify file size is logged
    logger.info.assert_called()
    log_message = logger.info.call_args[0][0]
    assert 'bytes' in log_message

    # Verify actual file size matches
    actual_size = backup_file.stat().st_size
    assert actual_size > 0
    assert str(actual_size) in log_message

    # Cleanup
    backup_file.unlink()
