from tempfile import TemporaryDirectory

import pytest
from sqlalchemy import select
from sqlalchemy.sql.functions import count as sql_count

from discord_core.cogs.music_helpers.common import SearchType
from discord_core.types.media_download import MediaDownload
from discord_core.types.media_request import MediaRequest
from discord_core.types.playlist import PlaylistItemWrite
from discord_core.types.search import SearchResult

from discord_db.database import Playlist, PlaylistItem

from tests.fakes.pipeline_fakes import (fake_envelope, yield_download_worker_download_exception,
                                        yield_fake_download_worker, yield_fake_search_client)
from tests.helpers import async_mock_session, fake_media_download, fake_source_dict
from tests.helpers import fake_engine, fake_context, fake_stores  #pylint:disable=unused-import
from tests.results_pod import ResultsPod


@pytest.mark.asyncio
async def test_playlist_add_flows_through_both_result_loops(fake_engine, fake_context, fake_stores):  #pylint:disable=redefined-outer-name
    s = fake_source_dict(fake_context)
    sd = MediaDownload(None, {'webpage_url': 'https://foo.example'}, s)
    pod = ResultsPod(fake_context, fake_stores, with_search=True,
                     download_worker_cls=yield_fake_download_worker(sd),
                     source_search_client=yield_fake_search_client(s)())
    playlist_id = (await fake_stores.playlist.create_playlist(fake_context['guild'].id, 'new-playlist')).id
    # What `!playlist item-add` does in the commands pod once it has found the playlist.
    await pod.enqueuer.generate_media_requests_from_search(
        fake_envelope(fake_context), 'https://foo.example', add_to_playlist=playlist_id)
    await pod.search_driver.run_once(pod.stop_event)
    await pod.loops.process_search_results()
    await pod.download_client.run(pod.stop_event)
    await pod.loops.process_download_results()
    async with async_mock_session(fake_engine) as db_session:
        assert (await db_session.execute(select(sql_count()).select_from(PlaylistItem))).scalar() == 1


@pytest.mark.asyncio
async def test_failed_history_download_deletes_the_history_item(fake_engine, fake_context, fake_stores):  #pylint:disable=redefined-outer-name
    '''A history item whose video can no longer be fetched is removed from the history playlist.'''
    with TemporaryDirectory() as tmp_dir:
        with fake_media_download(tmp_dir, fake_context=fake_context) as sd:
            pod = ResultsPod(fake_context, fake_stores, with_search=True,
                             download_worker_cls=yield_download_worker_download_exception())
            guild_id = fake_context['guild'].id
            await pod.open_player(guild_id, fake_context['channel'].id)
            # The broker's history worker records plays now; seed the history playlist directly.
            history_playlist_id = await fake_stores.playlist.ensure_history_playlist(guild_id)
            await fake_stores.playlist.record_history_item(
                history_playlist_id,
                PlaylistItemWrite(video_url=sd.webpage_url, title=sd.title, uploader=sd.uploader), 64)

            # What `!playlist queue 0` does in the commands pod: one request per history item.
            history_items = await fake_stores.playlist.list_items(history_playlist_id)
            bundle_uuid = await pod.broker_client.create_bundle(
                guild_id, fake_context['channel'].id, input_string='Channel History', has_search_banner=True)
            await pod.enqueuer.enqueue_media_requests(
                fake_envelope(fake_context),
                [MediaRequest(guild_id=guild_id, channel_id=fake_context['channel'].id,
                              requester_name='tyler', requester_id=1,
                              search_result=SearchResult(search_type=SearchType.YOUTUBE,
                                                         raw_search_string=item.video_url,
                                                         proper_name=item.title),
                              added_from_history=True, history_playlist_item_id=item.id)
                 for item in history_items],
                bundle_uuid)
            await pod.search_driver.run_once(pod.stop_event)
            await pod.loops.process_search_results()
            await pod.download_client.run(pod.stop_event)
            await pod.loops.process_download_results()
            async with async_mock_session(fake_engine) as db_session:
                assert (await db_session.execute(select(sql_count()).select_from(Playlist))).scalar() == 1
                assert (await db_session.execute(select(sql_count()).select_from(PlaylistItem))).scalar() == 0
