# How to contribute to a project

- Clone a repository
- Create a new [branch](#branches)
- [Deploy project](#project-deployment) and make a change
- [Create commit](#commits), push changes and create Pull Request
- Pass the code review
- [Task release process](#how-tasks-are-released)

## Code development

### Project deployment

The project can be started locally with the command. The sandbox in the docker-compose environment will up.
The tests run inside the app service.

```
docker-compose up -d --build
```

### Code testing

#### Unit

All tests can be run by the command:

```
docker-compose -f docker-compose.tests.yml up --build --exit-code-from=tests
```

### Code style

Please follow the linters in make lint.

### Making tickets/commits/branches

#### Branches

Branches should preferably be created from master branch.

```
git switch master # change to master
git pull # getting changes
git switch -c branch-name # creating a branch
```

#### Commits

The project has no strict requirements, but remember that commits will therefore be read by other developers and they can help them understand the changes in the project.

#### Tickets

## How tasks are released

Releases occur as needed. The semver rule is followed.
The project is published through poetry. The version is stored in pyproject.toml file.
