runNeighborhoodsPipeline([
    project: 'honeycomb-ci',
    tests: [
        'Dev/User environment Comparison': 'pipenv-devcheck',
        'Unit testing': 'python -m pytest',
        'Linting/Style Checking': "python -m flake8 --max-line-length 100"
    ],
])
