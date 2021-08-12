# Build documentation with MkDocs

OceanBase documentation is built with [MkDocs](https://www.mkdocs.org/). You can check [`mkdocs.yml`](mkdocs.yml) for more information.

## Requirements

Before installing dependencies, please make sure you have installed a recent version of Python 3 and pip.

Then you can run the following command in your terminal:

    $ pip install -r docs/requirements.txt

## Build the documentation

You can build the documentation by running the following command:

    $ mkdocs build

This will create a new directory to store the output files, which is `site/` by default.

## Start a server locally

You can start a server locally by running the following command:

    $ mkdocs serve

Open up http://127.0.0.1:8000/ in your browser, and you'll see the default home page.

## Modify pages

### Edit a page

If you want to modify the content of a page, you can edit the markdown file in `docs/` directory directly.

### Modify the layout of pages

To modify the layout of pages, you need to edit `mkdocs.yml`.

For configuration details, see [MkDocs User Guide](https://www.mkdocs.org/user-guide/configuration/).

**Note:** `docs/` is the default value of `docs_dir`, which means `docs/` is equivalent to `./` in `mkdocs.yml`.

## Contribute

See [How to contribute](CONTRIBUTING.md).
