from setuptools import setup, find_packages

with open("requirements.txt") as f:
    requires = [l.strip("\n") for l in f.readlines()]

setup(
    name="asyncify",
    version="0.0.1",
    packages=find_packages(),
    install_requires=requires,
    # 使用asyncify-cli管理
    entry_points='''
        [console_scripts]
        asyncify-cli=asyncify.cli.asyncifycli:asyncify_cli
    '''
)
