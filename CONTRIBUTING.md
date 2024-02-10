The development guide is located under the [docs](docs/README.md) folder.

# Contribution Guidelines

Welcome to [oceanbase]! We're thrilled that you'd like to contribute. Your help is essential for making it better.

## Getting Started

Before you start contributing, please make sure you have read and understood our [Code of Conduct](CODE_OF_CONDUCT.md).

### Fork the Repository

First, fork the [oceanbase repository](https://github.com/oceanbase/oceanbase) to your own GitHub account. This will create a copy of the project under your account.


### Clone Your Own Repository
```bash
git clone https://github.com/`your-github-name`/oceanbase
```
### Navigate to the Project Directory 📁
```bash
cd oceanbase
```
Create a new branch for your feature or bug fix:
```bash
 git checkout -b feature-branch
```

> feature-branch is the name of the branch where you will be making your changes. You can name this whatever you want.

Make your changes and commit them:
```bash
git add .
git commit -m "Description of your changes"
```
Push your changes to your fork:
```bash
git push origin feature-branch
```
Finally Click on `Compare & Pull request` to contribute on this repository.

### The Flow After You Create the Pull Request
After you create the pull request, a member of the Oceanbase team will review your changes and provide feedback. Once satisfied, they will merge your pull request.

And there are some CI checks to pass before your pull request can be merged. Currently, there are two types of CI checks:
- **Compile**: This check will compile the code on CentOS and Ubuntu.
- **Farm**: This check will run the unit tests and some mysql test cases.

> Note: If the farm failed and you think it is not related to your changes, you can ask the reviewer to re-run the farm or the reviewer will re-run the farm.

### The Flow After Your Pull Request is Merged
In default, the pull request is merged into develop branch which is the default branch of [oceanbase](https://github.com/oceanbase/oceanabse). We will merge develop into master branch periodically. So if you want to get the latest code, you can pull the master branch.

## Feature Developing
If you want to develop a new feature, you should create a [discussion](https://github.com/oceanbase/oceanbase/discussions/new/choose) first. If your idea is accepted, you can create a new issue and start to develop your feature and we will create a feature branch for you. After you finish your feature, you can create a pull request to merge your feature branch into oceanbase feature branch. The flow like below.

1. Create a [discussion](https://github.com/oceanbase/oceanbase/discussions/new/choose)
2. Create a new issue
3. Create a new feature branch on [oceanabse](https://github.com/oceanbase/oceanbase) for your feature
4. Make your changes and commit them
5. Push your changes to your fork
6. Create a pull request to merge your code into feature branch
7. After your pull request is merged, we will merge your feature branch into master
