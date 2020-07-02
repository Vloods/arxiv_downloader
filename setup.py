from setuptools import setup, find_packages

requirements = []
try:
    requirements = open('requirements.txt').read().splitlines()
    print('Requirements are:', requirements)
except:
    print('no requirements.txt found')

setup(
    name='arxiv_downloader',
    version='0.1',
    packages=find_packages(),
    url='',
    license='',
    author='vloods',
    author_email='lmvibe@outlook.com',
    description='Paper loader from arXiv',
    install_requires=requirements
)
