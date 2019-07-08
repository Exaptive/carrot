from setuptools import setup
from os.path import join, dirname

with open(join(dirname(__file__), 'carrot/version.py'), 'r') as f:
    exec(f.read())

with open (join(dirname(__file__), 'requirements.txt'), 'r') as f:
    install_requires = f.read().split("\n")

setup(
    name='carrot',
    version=__version__,
    url="https://github.com/Exaptive/carrot",
    license="BSD (3 clause)",
    author="Joshua Southerland",
    author_email="josh.southerland@exaptive.com",
    description="Carrot",
    long_description="Carrot is a cross-platform distributed work system.",
    packages=['carrot'],
    zip_safe=False,
    include_package_data=True,
    platforms='any',
    install_requires=install_requires,
    tests_require=[],
    test_suite='',
    classifiers=[])
