from discord_core.types.player_session import PlayerSession


def test_a_session_saved_with_the_old_queue_key_still_loads():
    '''Sessions written before the queue moved to the broker carry `queue`; it is dropped'''
    session = PlayerSession.model_validate({
        'guild_id': 1, 'voice_channel_id': 2, 'text_channel_id': 3, 'was_playing': True,
        'queue': [{'guild_id': 1, 'channel_id': 3, 'author_id': 4, 'author_name': 'a',
                   'search_string': 'x', 'download_file': True, 'added_from_history': False}],
    })
    assert session.was_playing is True
    assert 'queue' not in session.model_dump()


def test_a_session_defaults_to_not_playing():
    session = PlayerSession(guild_id=1, voice_channel_id=2, text_channel_id=3)
    assert session.was_playing is False
