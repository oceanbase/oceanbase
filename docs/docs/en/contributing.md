# OceanBase Contributing Guidelines

Thank you for considering contributing to OceanBase.

We welcome any type of contribution to the OceanBase community. You can contribute code, help our new users in the DingTalk group (Group No.: 33254054), [Slack group](https://join.slack.com/t/oceanbase/shared_invite/zt-1e25oz3ol-lJ6YNqPHaKwY_mhhioyEuw) or in [StackOverflow](https://stackoverflow.com/search?q=oceanbase&s=4b3eddc8-73a2-4d90-8039-95fcc24e6450), test releases, or improve our documentation.

If you are interested in contributing code to OceanBase, please read the following guidelines and follow the steps.

## Start off with the right issue

As a new contributor, you can start off by looking through our [good first issues](https://github.com/oceanbase/oceanbase/issues?q=is%3Aopen+is%3Aissue+label%3A%22good+first+issue%22). If none of them feels right for you, you can create a new issue when you find one. If you find an issue for you, please assign this issue to yourself in the issue topic and add the _developing_ label to indicate that this issue is being developed.

## Contribute code changes

1. Fork the OceanBase repository.
   
   1. Visit the [OceanBase GitHub repository](https://github.com/oceanbase/oceanbase).
   2. Click the **Fork** button in the upper-right corner to fork one branch.

2. Configure the local environment.

```bash
working_dir=$HOME/{your_workspace} # Define the working directory of your choice. 
user={github_account} # Make sure the user name is the same as your GitHub account. 
```

3. Git clone the code.

```bash
mkdir -p $working_dir
cd $working_dir
git clone git@github.com:$user/oceanbase.git

# Add the upstream branch.
cd $working_dir/oceanbase
git remote add upstream git@github.com:oceanbase/oceanbase.git
# Or choose: git remote add upstream https://github.com/oceanbase/oceanbase

# Set no_push for the upstream branch.
git remote set-url --push upstream no_push

# Check if the upstream works as expected.
git remote -v
```

4. Create a new branch.

```bash
# Check out the local master.
new_branch_name={issue_id} # Define the branch name. It is recommended that you use {issue+id} for the branch name. 
cd $working_dir/oceanbase
git fetch upstream
git checkout master
git rebase upstream/master
git checkout -b $new_branch_name
```

5. Finish all the developing tasks in the new branch, including the testing tasks.
6. Submit changes to your branch.

```
# Check the local status.
git status

# Add files for submission.
# Directly add all changes using `git add .`
git add <file> ... 
# In order to relate the pull request automatically to the issue, 
# it is recommended that the commit message contain "fixed #{issue_id}". 
git commit -m "fixed #{issue_id}: {your_commit_message}"

# Sync upstream before push.
git fetch upstream
git rebase upstream/master
git push -u origin $new_branch_name
```

7. Create a pull request.
   1. Visit your fork repository.
   2. Click the **Compare & pull request** button next to the {new_branch_name} to create a pull request.
8. Sign the [Contributor License Agreement (CLA)](https://cla-assistant.io/oceanbase/oceanbase) aggreement. The workflow can continue only after you sign the CLA aggreement.

![CLA](https://user-images.githubusercontent.com/5435903/204097095-6a19d2d1-ee0c-4fb6-be2d-77f7577d75d2.png#crop=0&crop=0&crop=1&crop=1&from=url&id=Mmj8a&margin=%5Bobject%20Object%5D&originHeight=271&originWidth=919&originalType=binary&ratio=1&rotation=0&showTitle=false&status=done&style=none&title=)

After you submit your updates, OceanBase will review and comment if needed. Once we approve your updates, the system will automatically run CI testing and stress testing. If no issues are found in the tests, your updates will be merged. Now you have successfully completed your contribution task and become one of our contributors.
