import setuptools

setuptools.setup(
    name='discord_bot',
    description='Discord Bot',
    author='Tyler D. North',
    author_email='ty_north@yahoo.com',
    install_requires=[
        'cryptography >= 2.9.2',
        'discord >= 1.0.1',
        'discord.py >= 1.3.4',
        'moviepy >= 1.0.3',
        'mysqlclient >= 1.4.6',
        'numpy >= 1.18.5',
        'PyMySQL >= 0.9.3',
        'PyNaCl >= 1.3.0',
        'python-twitter >= 3.5',
        'SQLAlchemy >= 1.3.13',
        'youtube-dl >= 2020.9.20',
    ],
    entry_points={
        'console_scripts' : [
            'discord-bot = discord_bot.run_bot:main',
        ]
    },
    packages=setuptools.find_packages(exclude=['tests']),
    version='1.3.9',
)
