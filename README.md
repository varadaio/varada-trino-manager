# Varada Trino Manager

## **Table of contents**

- [Install](#Install)
- [Usage](#Usage)
- [Connections](#Connections)
- [Configuration](#Configuration)

---

## Install

```
pip install git+https://github.com/varadaio/varada-trino-manager
```

Some distributions might have issue with building `cryptography` package, if you encounter this issue install using the flowing

```
CRYPTOGRAPHY_DONT_BUILD_RUST=1 pip install git+https://github.com/varadaio/varada-trino-manager
```

---

## Usage

```
my-machine:~ user# vtm

Usage: vtm [OPTIONS] COMMAND [ARGS]...

  Varada trino manager

Options:
  -v, --verbose  Be more verbose
  --help         Show this message and exit.

Commands:
  config     Config related commands
  connector  Connector related commands
  etc        More utilities
  logs       Logs related commands
  query      Query utility commands
  rules      Rules utility commands
  server     Server management related commands
  ssh        SSH related operations
```

---

## Connections

The app assumes an ssh agent is running, if you don't have one running please visit [here](https://kb.iu.edu/d/aeww)

---

## Configuration

The app looks for configuration in `~/.vtm` directory unless it's instructed otherwise by setting an environment variable named `VARADA_TRINO_MANAGER_DIR`.

The app configuration is a file named `config.json`, the file schema is as follows (chose the one that suits you)
```
my-machine:~ user# vtm config template
Simple:
{
  "coordinator": "coordinator.example.com",
  "workers": [
    "worker1.example.com",
    "worker2.example.com",
    "worker3.example.com"
  ],
  "port": 22,
  "username": "root"
}

With bastion and distribution:
{
  "coordinator": "coordinator.example.com",
  "workers": [
    "worker1.example.com",
    "worker2.example.com",
    "worker3.example.com"
  ],
  "port": 22,
  "username": "root",
  "bastion": {
    "hostname": "bastion.example.com",
    "port": 22,
    "username": "root"
  },
  "distribution": {
    "brand": "trino",
    "port": 8080
  }
}

brand can be either trino or presto
```