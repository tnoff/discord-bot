import setuptools

setuptools.setup(
    name='discord_bot',
    description='Discord Bot',
    author='Tyler D. North',
    author_email='ty_north@yahoo.com',
    install_requires=[
        'cryptography >= 2.9.2',
        'discord >= 1.0.1',
        'mysqlclient >= 1.4.6',
        'prettytable>=0.7.2',
        'PyMySQL >= 0.9.3',
        'PyNaCl >= 1.3.0',
        'python-twitter >= 3.5',
        'SQLAlchemy >= 1.3.13',
        'youtube-dl >= 2020.3.24',
    ],
    entry_points={
        'console_scripts' : [
            'discord-bot = discord_bot.run_bot:main',
            'twitter-bot = discord_bot.twitter:main',
        ]
    },
    packages=setuptools.find_packages(exclude=['tests']),
    version='0.2.21',
)
