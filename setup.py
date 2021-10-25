import setuptools

setuptools.setup(
    name='discord_bot',
    description='Discord Bot',
    author='Tyler D. North',
    author_email='ty_north@yahoo.com',
    install_requires=[
        # TODO move this to requires.txt
        # Then allow for additional files for plugins?
        'cryptography >= 2.9.2',
        'discord >= 1.0.1',
        'PyMySQL >= 1.0.2',
        'PyNaCl >= 1.4.0',
        'python-twitter >= 3.5',
        'pathlib >= 1.0.1',
        'SQLAlchemy >= 1.4.18',
        'yt-dlp >= 2021.10.10',
    ],
    entry_points={
        'console_scripts' : [
            'discord-bot = discord_bot.run_bot:main',
        ]
    },
    packages=setuptools.find_packages(exclude=['tests']),
    version='1.8.2',
)
