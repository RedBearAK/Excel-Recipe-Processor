"""Setup script for excel_recipe_processor package."""

from setuptools import setup, find_packages
import os

# Read version from _version.py
version_file = os.path.join('excel_recipe_processor', '_version.py')
version_info = {}
with open(version_file) as f:
    exec(f.read(), version_info)

# Read README for long description
readme_file = 'README.md'
long_description = ''
if os.path.exists(readme_file):
    with open(readme_file, 'r', encoding='utf-8') as f:
        long_description = f.read()

setup(
    name='excel_recipe_processor',
    version=version_info['__version__'],
    author=version_info['__author__'],
    author_email=version_info['__email__'],
    description=version_info['__description__'],
    long_description=long_description,
    long_description_content_type='text/markdown',
    packages=find_packages(),
    python_requires='>=3.7',
    install_requires=[
        # Add your dependencies here
        # 'pandas>=1.0.0',
        # 'pyyaml>=5.0.0',
    ],
    extras_require={
        'dev': [
            'pytest>=6.0.0',
            'pytest-cov>=2.0.0',
            'black>=20.0.0',
            'flake8>=3.8.0',
            'mypy>=0.800',
        ],
        'docs': [
            'sphinx>=3.0.0',
            'sphinx-rtd-theme>=0.5.0',
        ],
    },
    entry_points={
        'console_scripts': [
            'excel_recipe_processor=excel_recipe_processor.__main__:main',
        ],
    },
    classifiers=[
        'Development Status :: 3 - Alpha',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: MIT License',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.7',
        'Programming Language :: Python :: 3.8',
        'Programming Language :: Python :: 3.9',
        'Programming Language :: Python :: 3.10',
        'Programming Language :: Python :: 3.11',
    ],
)
