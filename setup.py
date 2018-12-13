from setuptools import setup, find_packages

setup(
    name='data_integration_pipelines',
    version='0.0.1',
    packages=find_packages(),
    url='http://www.curwsl.org',
    license='MIT License',
    author='thilinamad',
    author_email='madumalt@gamil.com',
    description='Data integration pipelines based on di_framework.',
    install_requires=['di_framework'],
    dependency_links=[
        'git+https://github.com/CUrW-SL/DI_Framework.git@master#egg=di_framework-0.0.1'
        'git+https://github.com/CUrW-SL/data_layer.git@master#egg=data_layer-0.0.1',
        'git+https://github.com/CUrW-SL/algo_wrapper.git@master#egg=algo_wrapper-1.0.0'
    ],
    zip_safe=True
)
