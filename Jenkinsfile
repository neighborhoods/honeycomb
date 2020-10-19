runNeighborhoodsPipeline([
    project: 'honeycomb-ci',
    tests: [
        'Dev/User environment Comparison': 'pipenv-devcheck',
        'Unit testing': 'python -m pytest test/',
        'Linting/Style Checking': "python -m flake8 honeycomb/ test/ --max-line-length 100"
    ],
])
