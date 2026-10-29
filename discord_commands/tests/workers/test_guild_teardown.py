import asyncio
from unittest.mock import AsyncMock, MagicMock

import pytest

from discord_core.cogs.music_helpers.common import MediaRequestLifecycleStage, SearchType
from discord_core.types.playlist_add_request import PlaylistAddRequest
from discord_core.types.search import SearchResult

from discord_downloader.interfaces.download_protocols import ClearGuildResult

from discord_commands.workers.guild_teardown import GuildTeardown

from tests.helpers import FakeGuild, fake_engine, fake_context, fake_source_dict, fake_stores  #pylint:disable=unused-import
from tests.results_pod import ResultsPod


def make_teardown(pod, **kwargs):
    return GuildTeardown(pod.download_client, pod.youtube_music_search_client, pod.broker_client,
                         pod.enqueuer, pod.logger, **kwargs)


async def tear_down(teardown, guild_id):
    '''Start a teardown and wait for it to finish.'''
    assert teardown.submit(guild_id)
    await teardown.close()


@pytest.mark.asyncio
async def test_search_queue_items_are_discarded(fake_context, fake_stores):  #pylint:disable=redefined-outer-name
    pod = ResultsPod(fake_context, fake_stores, with_search=True)
    request = fake_source_dict(fake_context)
    await pod.broker_client.register_request(request)
    await pod.youtube_music_search_client.submit(fake_context['guild'].id, request)

    await tear_down(make_teardown(pod), fake_context['guild'].id)

    assert request.lifecycle_stage == MediaRequestLifecycleStage.DISCARDED
    assert await pod.youtube_music_search_client.queue_size(fake_context['guild'].id) == 0


@pytest.mark.asyncio
async def test_download_queue_items_are_discarded(fake_context, fake_stores):  #pylint:disable=redefined-outer-name
    pod = ResultsPod(fake_context, fake_stores, with_search=True)
    request = fake_source_dict(fake_context)
    await pod.broker_client.register_request(request)
    await pod.download_client.submit(fake_context['guild'].id, request)

    await tear_down(make_teardown(pod), fake_context['guild'].id)

    assert request.lifecycle_stage == MediaRequestLifecycleStage.DISCARDED
    assert await pod.download_client.queue_size(fake_context['guild'].id) == 0


@pytest.mark.asyncio
async def test_each_queue_is_blocked_only_after_both_are_cleared():
    """A clear restores the items it keeps with put_nowait, which raises once the queue is blocked."""
    download_client, search_client = (
        MagicMock(clear_guild_queue=AsyncMock(return_value=ClearGuildResult(dropped=[])),
                  block_guild=AsyncMock()) for _ in range(2))
    order = MagicMock()
    order.attach_mock(download_client.clear_guild_queue, 'clear_download')
    order.attach_mock(search_client.clear_guild_queue, 'clear_search')
    order.attach_mock(download_client.block_guild, 'block_download')
    order.attach_mock(search_client.block_guild, 'block_search')
    broker_client = MagicMock(list_bundles_for_guild=AsyncMock(return_value=[]))

    await tear_down(GuildTeardown(download_client, search_client, broker_client, MagicMock(), MagicMock()), 7)

    assert [call[0] for call in order.mock_calls] == [
        'clear_download', 'clear_search', 'block_download', 'block_search']
    download_client.block_guild.assert_awaited_once_with(7)


@pytest.mark.asyncio
async def test_the_guilds_bundles_are_deleted_and_another_guilds_are_not(fake_context, fake_stores):  #pylint:disable=redefined-outer-name
    pod = ResultsPod(fake_context, fake_stores, with_search=True)
    guild_id = fake_context['guild'].id
    own = await pod.broker_client.create_bundle(guild_id, fake_context['channel'].id)
    other_guild = FakeGuild()
    other = await pod.broker_client.create_bundle(other_guild.id, fake_context['channel'].id)

    await tear_down(make_teardown(pod), guild_id)

    assert own not in await pod.broker_client.list_bundles_for_guild(guild_id)
    assert other in await pod.broker_client.list_bundles_for_guild(other_guild.id)


@pytest.mark.asyncio
async def test_a_bundle_with_a_playlist_add_still_queued_is_kept(fake_context, fake_stores):  #pylint:disable=redefined-outer-name
    """A playlist add has no download, so the clear leaves it queued and its bundle is not deleted."""
    pod = ResultsPod(fake_context, fake_stores, with_search=True)
    guild_id = fake_context['guild'].id
    bundle_uuid = await pod.broker_client.create_bundle(
        guild_id, fake_context['channel'].id, input_string='https://example.com/playlist',
        has_search_banner=True)
    playlist_add = PlaylistAddRequest(
        guild_id=guild_id, channel_id=fake_context['channel'].id, requester_name='tester',
        requester_id=fake_context['author'].id, playlist_id=1,
        search_result=SearchResult(search_type=SearchType.YOUTUBE, raw_search_string='https://example.com/video'))
    playlist_add.bundle_uuid = bundle_uuid
    await pod.broker_client.register_request(playlist_add)
    await pod.youtube_music_search_client.submit(guild_id, playlist_add)

    await tear_down(make_teardown(pod), guild_id)

    assert bundle_uuid in await pod.broker_client.list_bundles_for_guild(guild_id)
    assert await pod.youtube_music_search_client.queue_size(guild_id) == 1


@pytest.mark.asyncio
async def test_bundles_the_worker_pods_report_as_kept_are_not_deleted():
    """The predicate runs on the worker pod, so the pod reports which bundles it kept."""
    download_client = MagicMock(clear_guild_queue=AsyncMock(
        return_value=ClearGuildResult(dropped=[], preserved_bundle_uuids={'keep-bundle'})),
        block_guild=AsyncMock())
    search_client = MagicMock(clear_guild_queue=AsyncMock(
        return_value=ClearGuildResult(dropped=[], preserved_bundle_uuids={'keep-too'})),
        block_guild=AsyncMock())
    broker_client = MagicMock(list_bundles_for_guild=AsyncMock(
        return_value=['keep-bundle', 'keep-too', 'drop-bundle']), delete_bundle=AsyncMock())

    await tear_down(GuildTeardown(download_client, search_client, broker_client, MagicMock(),
                                  MagicMock()), 4242)

    broker_client.delete_bundle.assert_awaited_once_with('drop-bundle')


@pytest.mark.asyncio
async def test_a_failure_is_logged_and_does_not_reach_the_caller():
    download_client = MagicMock(clear_guild_queue=AsyncMock(side_effect=RuntimeError('worker down')))
    logger = MagicMock()
    teardown = GuildTeardown(download_client, MagicMock(), MagicMock(), MagicMock(), logger)

    await tear_down(teardown, 4242)

    logger.exception.assert_called_once()
    assert '4242' in logger.exception.call_args.args[0]


@pytest.mark.asyncio
async def test_a_request_that_cannot_be_discarded_does_not_stop_the_rest():
    first, second = MagicMock(uuid='a'), MagicMock(uuid='b')
    download_client = MagicMock(clear_guild_queue=AsyncMock(
        return_value=ClearGuildResult(dropped=[first, second])), block_guild=AsyncMock())
    search_client = MagicMock(clear_guild_queue=AsyncMock(return_value=ClearGuildResult(dropped=[])),
                              block_guild=AsyncMock())
    enqueuer = MagicMock(push_state=AsyncMock(side_effect=[RuntimeError('unknown request'), None]))
    broker_client = MagicMock(list_bundles_for_guild=AsyncMock(return_value=[]))
    logger = MagicMock()

    await tear_down(GuildTeardown(download_client, search_client, broker_client, enqueuer, logger), 7)

    assert enqueuer.push_state.await_count == 2
    logger.exception.assert_called_once()
    assert 'a' in logger.exception.call_args.args[0]
    download_client.block_guild.assert_awaited_once_with(7)


@pytest.mark.asyncio
async def test_a_second_request_for_a_guild_being_torn_down_is_dropped():
    release = asyncio.Event()

    async def clear(*_args, **_kwargs):
        await release.wait()
        return ClearGuildResult(dropped=[])

    download_client = MagicMock(clear_guild_queue=clear, block_guild=AsyncMock())
    search_client = MagicMock(clear_guild_queue=AsyncMock(return_value=ClearGuildResult(dropped=[])),
                              block_guild=AsyncMock())
    broker_client = MagicMock(list_bundles_for_guild=AsyncMock(return_value=[]))
    teardown = GuildTeardown(download_client, search_client, broker_client, MagicMock(), MagicMock())

    assert teardown.submit(7)
    assert not teardown.submit(7)
    assert teardown.submit(8)  # another guild is not held up by it
    release.set()
    await teardown.close()
    # Done, so the guild can be torn down again.
    assert teardown.submit(7)
    await teardown.close()


@pytest.mark.asyncio
async def test_close_cancels_a_teardown_that_outlasts_the_grace():
    started = asyncio.Event()

    async def clear(*_args, **_kwargs):
        started.set()
        await asyncio.Event().wait()

    teardown = GuildTeardown(MagicMock(clear_guild_queue=clear), MagicMock(), MagicMock(), MagicMock(),
                             MagicMock(), grace=0.01)
    teardown.submit(7)
    await started.wait()

    await teardown.close()

    assert not teardown._tasks  #pylint:disable=protected-access


@pytest.mark.asyncio
async def test_close_with_nothing_running_returns():
    await GuildTeardown(MagicMock(), MagicMock(), MagicMock(), MagicMock(), MagicMock()).close()
