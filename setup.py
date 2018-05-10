from setuptools import find_packages, setup

version_info = {}
with open('mensor/_version.py') as version_file:
    exec(version_file.read(), version_info)

setup(
    name='mensor',
    description="A dynamic graph-based metric computation engine.",
    version=version_info['__version__'],
    author=version_info['__author__'],
    author_email=version_info['__author_email__'],
    packages=find_packages(),
    install_requires=version_info['__dependencies__'],
    classifiers=[
        'Development Status :: 3 - Alpha',
        'Environment :: Console',
        'Intended Audience :: Developers',
        'Intended Audience :: Information Technology',
        'Intended Audience :: Science/Research',
        'License :: OSI Approved :: MIT License',
        'Programming Language :: Python :: 3.4',
        'Programming Language :: Python :: 3.5',
        'Programming Language :: Python :: 3.6',
    ]
)
