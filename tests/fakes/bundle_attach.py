'''The in-memory brokers' way of attaching a registered request to a bundle they hold.'''
from discord_broker.workers.media_bundle import BundleRenderer


class BundleAttachMixin:
    '''For a MediaBrokerBase subclass that keeps its bundle states in ``self._bundles``.'''

    async def _attach_to_bundle(self, media_request):
        '''If the request belongs to a known bundle, attach it.

        Loading the renderer rebuilds the table from BundleState, mutates it via
        add_media_request, and we keep the (mutated) state in the dict.
        '''
        bundle_uuid = media_request.bundle_uuid
        if bundle_uuid and bundle_uuid in self._bundles:
            renderer = BundleRenderer(self._bundles[bundle_uuid])
            renderer.add_media_request(media_request)
            self._bundles[bundle_uuid] = renderer.state
            await self._maybe_render_bundle(media_request)
