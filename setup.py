import setuptools

setuptools.setup(
    name='discord_bot',
    description='Discord Bot',
    author='Tyler D. North',
    author_email='ty_north@yahoo.com',
    install_requires=[
        'discord >= 1.0.1'
    ],
    entry_points={
        'console_scripts' : [
            'discord-bot = discord_bot.run_bot:main'
        ]
    },
    packages=setuptools.find_packages(exclude=['tests']),
    version='0.0.4',
)
