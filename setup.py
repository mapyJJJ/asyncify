from setuptools import setup, find_packages

setup(
    name="asyncify",
    version="0.0.1",
    packages=find_packages(),
    install_requires=[
        "Click",
    ],
    entry_points='''
        [console_scripts]
        asyncify-cli=asyncify.cli.asyncifyctl:asyncify_cli
    '''
)
