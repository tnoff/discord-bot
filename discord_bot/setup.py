import setuptools

setuptools.setup(
    name='discord_bot',
    description='Discord Bot',
    author='Tyler D. North',
    author_email='ty_north@yahoo.com',
    install_requires=[
        'discord >= 1.0.1',
        'mysqlclient >= 1.4.6',
        'PyMySQL >= 0.9.3',
        'SQLAlchemy >= 1.3.13',
    ],
    entry_points={
        'console_scripts' : [
            'discord-bot = discord_bot.run_bot:main'
        ]
    },
    packages=setuptools.find_packages(exclude=['tests']),
    version='0.0.15',
)
