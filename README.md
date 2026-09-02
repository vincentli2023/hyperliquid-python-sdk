# hyperliquid-python-sdk

<div align="center">

[![Dependencies Status](https://img.shields.io/badge/dependencies-up%20to%20date-brightgreen.svg)](https://github.com/hyperliquid-dex/hyperliquid-python-sdk/pulls?utf8=%E2%9C%93&q=is%3Apr%20author%3Aapp%2Fdependabot)

[![Code style: black](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/psf/black)
[![Security: bandit](https://img.shields.io/badge/security-bandit-green.svg)](https://github.com/PyCQA/bandit)
[![Pre-commit](https://img.shields.io/badge/pre--commit-enabled-brightgreen?logo=pre-commit&logoColor=white)](https://github.com/hyperliquid-dex/hyperliquid-python-sdk/blob/master/.pre-commit-config.yaml)
[![Semantic Versions](https://img.shields.io/badge/%20%20%F0%9F%93%A6%F0%9F%9A%80-semantic--versions-e10079.svg)](https://github.com/hyperliquid-dex/hyperliquid-python-sdk/releases)
[![License](https://img.shields.io/pypi/l/hyperliquid-python-sdk)](https://github.com/hyperliquid-dex/hyperliquid-python-sdk/blob/master/LICENSE.md)

SDK for Hyperliquid API trading with Python.

</div>

## Installation
```bash
pip install --no-cache-dir --upgrade \
  git+https://git@github.com/vincentli2023/hyperliquid-python-sdk.git@master
```
## Configuration 

- Set the public key as the `account_address` in examples/config.json.
- Set your private key as the `secret_key` in examples/config.json.
- See the example of loading the config in examples/example_utils.py

### [Optional] Generate a new API key for an API Wallet
Generate and authorize a new API private key on https://app.hyperliquid.xyz/API, and set the API wallet's private key as the `secret_key` in examples/config.json. Note that you must still set the public key of the main wallet *not* the API wallet as the `account_address` in examples/config.json

## Usage Examples
```python
from hyperliquid.info import Info
from hyperliquid.utils import constants

info = Info(constants.TESTNET_API_URL, skip_ws=True)
user_state = info.user_state("0xcd5051944f780a621ee62e39e493c489668acf4d")
print(user_state)
```
See [examples](examples) for more complete examples. You can also checkout the repo and run any of the examples after configuring your private key e.g. 
```bash
cp examples/config.json.example examples/config.json
vim examples/config.json
python examples/basic_order.py
```

## HIP-4 outcome markets and websocket post (fork additions)

Outcome (prediction) markets trade as spot-like coins named `#<encoding>` with `encoding = 10 * outcome + side`
(`#13380` is outcome 1338 side 0, usually *Yes*; `#13381` is *No*). Balances show up in `spotClearinghouseState`
as `+<encoding>` tokens and orders use asset id `100_000_000 + encoding`. Helpers live in `hyperliquid.utils.outcome`.

```python
info = Info(outcome_markets=True)               # default False: no extra request, tables unchanged
info.outcome_label("#13460")                    # 'xyz:SILVER above 64.128 at 2026-09-02 21:00 UTC? Yes'
info.outcome_settle_time_ms("#13460")           # 1788382800000
info.subscribe({"type": "l2Book", "coin": "#13460"}, print)   # '#' coins register on demand
exchange.order("#13460", True, 100, 0.43, {"limit": {"tif": "Ioc"}})   # whole shares, 5 significant figures
exchange.split_outcome(1346, 100); exchange.merge_outcome(1346)        # merge_outcome(None) = merge max
exchange.merge_question(195); exchange.negate_outcome(195, 1361, 12.5)
```

`Exchange(..., ws_manager=info.ws_manager)` sends signed actions as `{"method": "post"}` over an existing
websocket instead of HTTP; the payload is identical. `WebsocketManager.post()` raises `WebsocketPostError` when the
socket is not ready, the send fails, the reply times out or the connection closes while waiting, and it never
resends. A timeout means the action's outcome is unknown: query state before retrying.

Known limits: `orderUpdates` can only be subscribed for one address per connection (its identifier carries no
address); settled outcomes disappear from `outcomeMeta`, so labels fall back to the raw coin name; outcome sizes
are whole shares and prices carry 5 significant figures.

## Getting started with contributing to this repo

1. Download `Poetry`: https://python-poetry.org/. 
   - Note that in the install script you might have to set `symlinks=True` in `venv.EnvBuilder`.
   - Note that Poetry v2 is not supported, so you'll need to specify a specific version e.g. curl -sSL https://install.python-poetry.org | POETRY_VERSION=1.4.1 python3 - 

2. Point poetry to correct version of python. For development we require python 3.10 exactly. Some dependencies have issues on 3.11, while older versions don't have correct typing support.
`brew install python@3.10 && poetry env use /opt/homebrew/Cellar/python@3.10/3.10.16/bin/python3.10`

3. Install dependencies:

```bash
make install
```

### Makefile usage

CLI commands for faster development. See `make help` for more details.

```bash
check-safety          Run safety checks on dependencies
cleanup               Cleanup project
install               Install dependencies from poetry.lock
install-types         Find and install additional types for mypy
lint                  Alias for the pre-commit target
lockfile-update       Update poetry.lock
lockfile-update-full  Fully regenerate poetry.lock
poetry-download       Download and install poetry
pre-commit            Run linters + formatters via pre-commit, run "make pre-commit hook=black" to run only black
test                  Run tests with pytest
update-dev-deps       Update development dependencies to latest versions
```

## Releases

You can see the list of available releases on the [GitHub Releases](https://github.com/hyperliquid-dex/hyperliquid-python-sdk/releases) page.

We follow the [Semantic Versions](https://semver.org/) specification and use [`Release Drafter`](https://github.com/marketplace/actions/release-drafter). As pull requests are merged, a draft release is kept up-to-date listing the changes, ready to publish when you’re ready. With the categories option, you can categorize pull requests in release notes using labels.

### List of labels and corresponding titles

|               **Label**               |  **Title in Releases**  |
| :-----------------------------------: | :---------------------: |
|       `enhancement`, `feature`        |        Features         |
| `bug`, `refactoring`, `bugfix`, `fix` |  Fixes & Refactoring    |
|       `build`, `ci`, `testing`        |  Build System & CI/CD   |
|              `breaking`               |    Breaking Changes     |
|            `documentation`            |     Documentation       |
|            `dependencies`             |  Dependencies updates   |

### Building and releasing

Building a new version of the application contains steps:

- Bump the version of your package with `poetry version <version>`. You can pass the new version explicitly, or a rule such as `major`, `minor`, or `patch`. For more details, refer to the [Semantic Versions](https://semver.org/) standard.
- Make a commit to `GitHub`
- Create a `GitHub release`
- `poetry publish --build`

## License

This project is licensed under the terms of the `MIT` license. See [LICENSE](LICENSE.md) for more details.

```bibtex
@misc{hyperliquid-python-sdk,
  author = {Hyperliquid},
  title = {SDK for Hyperliquid API trading with Python.},
  year = {2024},
  publisher = {GitHub},
  journal = {GitHub repository},
  howpublished = {\url{https://github.com/hyperliquid-dex/hyperliquid-python-sdk}}
}
```

## Credits

This project was generated with [`python-package-template`](https://github.com/TezRomacH/python-package-template).
