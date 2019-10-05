# mantis-docs

## Development

### Prerequisites

1. python
1. pymdownx.arithmatex (`pip install pymdown-extensions`)
1. mkdocs (`pip install mkdocs`)

### Previewing

```sh
$ mkdocs serve
```

## Releasing

```sh
$ mkdocs gh-deploy
```

This will build the mkdocs markdown into a static site and deploy it to Github pages
via the `gh-pages` branch of this repo. The site will be available at https://netflix.github.io/mantis.
