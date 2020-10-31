from distutils.core import setup
from setuptools import find_packages
setup(
    name='rsockets2',         # How you named your package folder (MyLib)
    # Chose the same as "name"
    packages=find_packages(include=['rsockets2', 'rsockets2.*']),
    version='0.1',      # Start with a small number and increase it with every change you make
    # Chose a license from here: https://help.github.com/articles/licensing-a-repository
    license='apache-2.0',
    # Give a short description about your library
    description='Not sophisticated implementation of the RSockets protcol as Client',
    author='Jascha Riedel',                   # Type in your name
    author_email='jaschelite@googlemail.com',      # Type in your E-Mail
    # Provide either the link to your github or to your website
    url='https://github.com/freelancer1845/rsockets2-try',
    # download_url = 'https://github.com/user/reponame/archive/v_01.tar.gz',    # I explain this later on
    # Keywords that define your package best
    keywords=['RSockets', 'Client', 'ReactiveX'],
    install_requires=[            # I get to this in a second
        'rx',
        'websocket_client',
    ],
    classifiers=[
        # Chose either "3 - Alpha", "4 - Beta" or "5 - Production/Stable" as the current state of your package
        'Development Status :: 3 - Alpha',
        # Define that your audience are developers
        'Intended Audience :: Developers',
        'Topic :: Software Development :: Build Tools',
        'License :: OSI Approved :: Apache Software License',   # Again, pick a license
        # Specify which pyhton versions that you want to support
        'Programming Language :: Python :: 3.6',
        'Programming Language :: Python :: 3.7',
        'Programming Language :: Python :: 3.8',
        'Programming Language :: Python :: 3.9'
    ],
)
