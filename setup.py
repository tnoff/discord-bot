import setuptools
import os

THIS_DIR = os.path.dirname(__file__)
REQUIREMENTS_FILES = [os.path.join(THIS_DIR, 'requirements.txt')]

for root, dirs, files in os.walk(os.path.join(THIS_DIR, 'cogs/plugins')):
    for name in files:
        if 'requirements.txt' in name.lower():
            REQUIREMENTS_FILES.append(os.path.join(root, name))

required = []
for file_name in REQUIREMENTS_FILES:
    # Not sure why but tox seems to miss the file here
    # So add the check
    if os.path.exists(file_name):
        with open(file_name) as f:
            required += f.read().splitlines()

setuptools.setup(
    name='discord_bot',
    description='Discord Bot',
    author='Tyler D. North',
    author_email='ty_north@yahoo.com',
    install_requires=required,
    entry_points={
        'console_scripts' : [
            'discord-bot = discord_bot.run_bot:main',
        ]
    },
    packages=setuptools.find_packages(exclude=['tests']),
    version='1.8.5',
)
