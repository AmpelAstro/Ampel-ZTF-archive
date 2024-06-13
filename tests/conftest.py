
pytest_plugins = ["tests.fixtures"]


def pytest_addoption(parser):
    parser.addoption(
        "--integration",
        action="store_true",
        default=False,
        help="run docker-based integration tests",
    )
    parser.addoption(
        "--no-integration",
        dest="integration",
        action="store_false",
        help="disable docker-based integration tests",
    )
