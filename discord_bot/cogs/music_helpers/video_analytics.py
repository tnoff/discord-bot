from datetime import datetime, timezone
from functools import partial
from typing import Optional, Callable
from uuid import uuid4

from sqlalchemy.orm import Session

from discord_bot.database import VideoRequestAnalytics
from discord_bot.cogs.music_helpers.source_dict import SourceDict
from discord_bot.cogs.music_helpers.source_download import SourceDownload
from discord_bot.utils.sql_retry import retry_database_commands


class VideoAnalyticsTracker:
    '''
    Tracks video request analytics for usage pattern analysis
    '''

    def __init__(self, session_generator: Callable[[], Session]):
        '''
        Initialize analytics tracker

        Args:
            session_generator: Function that returns a database session
        '''
        self.session_generator = session_generator

    def create_request_record(self, source_dict: SourceDict, cache_hit_pre_queue: bool = False) -> str:
        '''
        Create initial analytics record for a video request

        Args:
            source_dict: Source dictionary with request information
            cache_hit_pre_queue: Whether video was found in cache before download queue

        Returns:
            analytics_id: Unique ID for this analytics record
        '''
        def _create_record(db_session: Session):
            analytics_record = VideoRequestAnalytics(
                server_id=str(source_dict.guild_id),
                search_string=source_dict.search_string,
                search_type=source_dict.search_type.value if source_dict.search_type else 'unknown',
                cache_hit_pre_queue=cache_hit_pre_queue,
                cache_hit_post_queue=False,
                download_attempted=not cache_hit_pre_queue,  # If cache hit, no download needed
                download_successful=cache_hit_pre_queue,     # If cache hit, consider successful
                created_at=datetime.now(timezone.utc)
            )
            db_session.add(analytics_record)
            db_session.commit()
            return analytics_record.id

        if self.session_generator:
            with self.session_generator() as db_session:
                return retry_database_commands(db_session, partial(_create_record, db_session))
        return str(uuid4())  # Fallback ID if no database

    def update_cache_hit_post_queue(self, analytics_id: str, video_cache_data):
        '''
        Update record when video is found in cache during download (yt-dlp filter)

        Args:
            analytics_id: ID of analytics record to update
            video_cache_data: VideoCache object with cached file information
        '''
        def _update_record(db_session: Session):
            record = db_session.query(VideoRequestAnalytics).filter(
                VideoRequestAnalytics.id == analytics_id
            ).first()
            if record:
                record.cache_hit_post_queue = True
                record.download_attempted = False  # Download was not needed
                record.download_successful = True  # Cache hit is successful
                # Update video details from cache
                if video_cache_data:
                    record.video_url = video_cache_data.video_url
                    record.video_title = video_cache_data.title
                    record.video_id = video_cache_data.video_id
                    record.extractor = video_cache_data.extractor
                db_session.commit()

        if self.session_generator:
            with self.session_generator() as db_session:
                retry_database_commands(db_session, partial(_update_record, db_session))

    def update_download_result(self, analytics_id: str, source_download: Optional[SourceDownload],
                              success: bool):
        '''
        Update record with download results

        Args:
            analytics_id: ID of analytics record to update
            source_download: SourceDownload object if successful, None if failed
            success: Whether download was successful
        '''
        def _update_record(db_session: Session):
            record = db_session.query(VideoRequestAnalytics).filter(
                VideoRequestAnalytics.id == analytics_id
            ).first()
            if record:
                record.download_successful = success
                # Update video details if download was successful
                if success and source_download:
                    record.video_url = source_download.webpage_url
                    record.video_title = source_download.title
                    record.video_id = source_download.id
                    record.extractor = source_download.extractor
                db_session.commit()

        if self.session_generator:
            with self.session_generator() as db_session:
                retry_database_commands(db_session, partial(_update_record, db_session))

    def get_analytics_summary(self) -> dict:
        '''
        Get summary analytics for metrics

        Returns:
            Dictionary with analytics counts
        '''
        def _get_summary(db_session: Session):
            # Total requests
            total_requests = db_session.query(VideoRequestAnalytics).count()

            # Cache hit rates
            pre_queue_cache_hits = db_session.query(VideoRequestAnalytics).filter(
                VideoRequestAnalytics.cache_hit_pre_queue == True
            ).count()

            post_queue_cache_hits = db_session.query(VideoRequestAnalytics).filter(
                VideoRequestAnalytics.cache_hit_post_queue == True
            ).count()

            # Download success rate
            successful_downloads = db_session.query(VideoRequestAnalytics).filter(
                VideoRequestAnalytics.download_successful == True
            ).count()

            attempted_downloads = db_session.query(VideoRequestAnalytics).filter(
                VideoRequestAnalytics.download_attempted == True
            ).count()

            # Search type breakdown
            search_type_counts = {}
            search_types = db_session.query(
                VideoRequestAnalytics.search_type,
                db_session.query(VideoRequestAnalytics).filter(
                    VideoRequestAnalytics.search_type == VideoRequestAnalytics.search_type
                ).count().label('count')
            ).distinct().all()

            for search_type, count in search_types:
                search_type_counts[search_type] = count

            return {
                'total_requests': total_requests,
                'pre_queue_cache_hits': pre_queue_cache_hits,
                'post_queue_cache_hits': post_queue_cache_hits,
                'successful_downloads': successful_downloads,
                'attempted_downloads': attempted_downloads,
                'search_type_counts': search_type_counts,
                'pre_queue_cache_hit_rate': pre_queue_cache_hits / max(total_requests, 1),
                'post_queue_cache_hit_rate': post_queue_cache_hits / max(total_requests, 1),
                'overall_cache_hit_rate': (pre_queue_cache_hits + post_queue_cache_hits) / max(total_requests, 1),
                'download_success_rate': successful_downloads / max(attempted_downloads, 1)
            }

        if self.session_generator:
            with self.session_generator() as db_session:
                return retry_database_commands(db_session, partial(_get_summary, db_session))
        return {}

    def get_video_request_frequency(self, limit: int = 100) -> list:
        '''
        Get most frequently requested videos

        Args:
            limit: Maximum number of results to return

        Returns:
            List of tuples: (video_url, request_count, video_title)
        '''
        def _get_frequency(db_session: Session):
            # Query for video request frequency
            results = db_session.query(
                VideoRequestAnalytics.video_url,
                db_session.query(VideoRequestAnalytics).filter(
                    VideoRequestAnalytics.video_url == VideoRequestAnalytics.video_url
                ).count().label('request_count'),
                VideoRequestAnalytics.video_title
            ).filter(
                VideoRequestAnalytics.video_url.isnot(None)
            ).group_by(
                VideoRequestAnalytics.video_url
            ).order_by(
                db_session.query(VideoRequestAnalytics).filter(
                    VideoRequestAnalytics.video_url == VideoRequestAnalytics.video_url
                ).count().desc()
            ).limit(limit).all()

            return [(result.video_url, result.request_count, result.video_title) for result in results]

        if self.session_generator:
            with self.session_generator() as db_session:
                return retry_database_commands(db_session, partial(_get_frequency, db_session))
        return []
