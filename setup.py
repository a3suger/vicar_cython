from setuptools import setup, Extension

module = Extension ('vicar', sources=['vicar.pyx'])

setup(
    name='vicar_cython',
    version='1.0',
    author='a3suger',
    author_email='akira@cc.tsukuba.ac.jp',
    ext_modules=[module]
)
