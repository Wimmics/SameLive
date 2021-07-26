try:
    from setuptools import setup, find_packages
    from setuptools.command.install import install as _install
except ImportError:
    from distutils.core import setup


class Install(_install):
    def run(self):
        _install.do_egg_install(self)

setup(
    name='SameLive',
    packages=["samelive"],
    version='1.0.0',
    author='Raphaël Gazzotti',
    author_email='raphael.gazzotti@inria.fr',
    cmdclass={'install': Install},
    install_requires=['tqdm', 'requests', 'SPARQLWrapper'],
    setup_requires=[]
)
