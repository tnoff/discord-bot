from tempfile import TemporaryDirectory

from tests.helpers import fake_source_download, generate_fake_context

def test_source_download_with_cache():
    with TemporaryDirectory() as tmp_dir:
        fake_context = generate_fake_context()
        with fake_source_download(tmp_dir, fake_context=fake_context) as x:
            original_file_path = x.file_path
            x.ready_file()
            assert str(x) == x.webpage_url  # pylint: disable=no-member
            assert str(x.file_path) != str(original_file_path)
            assert f'/{fake_context["guild"].id}/' in str(x.file_path)
            x.delete()
            assert not x.file_path.exists()
            assert original_file_path.exists()
