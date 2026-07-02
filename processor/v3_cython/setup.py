from setuptools import Extension, setup
from Cython.Build import cythonize


extensions = [
    Extension(
        name="processor",
        sources=["processor.pyx"],
        extra_compile_args=["-O3", "-fopenmp"],
        extra_link_args=["-fopenmp"],
    )
]


setup(
    name="hpc-cython-processor",
    ext_modules=cythonize(extensions, compiler_directives={"language_level": "3"}),
)
