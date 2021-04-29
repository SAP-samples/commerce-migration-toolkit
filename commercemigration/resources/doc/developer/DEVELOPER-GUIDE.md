# CMT - Developer Guide

## Quick Start

To install the Commerce Migration Toolkit, follow these steps:

Add the following extensions to your localextensions.xml:
```
<extension name="commercemigration"/>
<extension name="commercemigrationhac"/>
```

Make sure you add the source db driver to commercemigration/lib if necessary.

Use the following sample configuration and add it to your local.properties file:

```
migration.ds.source.db.driver=com.mysql.jdbc.Driver
migration.ds.source.db.url=jdbc:mysql://localhost:3600/localdev?useConfigs=maxPerformance&characterEncoding=utf8&useTimezone=true&serverTimezone=UTC&nullCatalogMeansCurrent=true
migration.ds.source.db.username=[user]
migration.ds.source.db.password=[password]
migration.ds.source.db.tableprefix=
migration.ds.source.db.schema=localdev

migration.ds.target.db.driver=${db.driver}
migration.ds.target.db.url=${db.url}
migration.ds.target.db.username=${db.username}
migration.ds.target.db.password=${db.password}
migration.ds.target.db.tableprefix=${db.tableprefix}
migration.ds.target.db.catalog=${db.catalog}
migration.ds.target.db.schema=dbo

```


## Contributing to the Commerce Migration Toolkit

To contribute to the Commerce Migration Toolkit, follow these steps:

1. Fork this repository;
2. Create a branch: `git checkout -b <branch_name>`;
3. Make your changes and commit them: `git commit -m '<commit_message>'`;
4. Push to the original branch: `git push origin <project_name>/<location>`;
5. Create the pull request.

Alternatively, see the GitHub documentation on [creating a pull request](https://help.github.com/en/github/collaborating-with-issues-and-pull-requests/creating-a-pull-request).
