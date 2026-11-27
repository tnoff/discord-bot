'''The media-search wire types are importable without a provider SDK.'''
from ci._image_deps import measure


def test_wire_types_pull_in_no_provider_sdk():
    '''Both sides of the seam serialise through these types, so neither may inherit spotipy or googleapiclient.'''
    imported = set(measure('discord_core.types.media_search')['packages'])
    assert 'spotipy' not in imported
    assert 'googleapiclient' not in imported
