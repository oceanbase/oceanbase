---
name: 🐞 Bug report
about: Create a report to help us improve
title: "[Bug]: "
labels: ["type: bug"]
---

name: "🕷️ Bug report"
description: Report errors or unexpected behavior
labels:
  - bug
body:
  - type: checkboxes
    attributes:
      label: Self Checks
      description: "To make sure we get to you in time, please check the following :)"
      options:
        - label: I have read the [Contributing Guide](https://github.com/oceanbase/oceanbase/blob/develop/CONTRIBUTING.md).
          required: true
        - label: This is only for bug report, if you would like to ask a question, please head to [Discussions](https://github.com/oceanbase/oceanbase/discussions/categories/general).
          required: true
        - label: I have searched for existing issues [search for existing issues](https://github.com/oceanbase/oceanbase/issues), including closed ones.
          required: true
        - label: I confirm that I am using English to submit this report, otherwise it will be closed.
          required: true
        - label: 【中文用户 & Non English User】请使用英语提交，否则会被关闭 ：）
          required: true
        - label: "Please do not modify this template :) and fill in all the required fields."
          required: true

  - type: input
    attributes:
      label: OceanBase version
      description: See about section in OceanBase console
    validations:
      required: true

  - type: dropdown
    attributes:
      label: Self Hosted
      description: How / Where was OceanBase installed from?
      multiple: true
      options:
        - Self Hosted (OBD)
        - Self Hosted (OCP)
        - Self Hosted (Desktop)
        - Self Hosted (Docker)
        - Self Hosted (Source)
        - Self Hosted (Other)
    validations:
      required: true

  - type: textarea
    attributes:
      label: Environment
      description: Please provide your system environment information
      placeholder: |
        OS Version and CPU Arch( uname -a ):
        OB Version( LD_LIBRARY_PATH=../lib:$LD_LIBRARY_PATH ./observer -V ):
    validations:
      required: true

  - type: textarea
    attributes:
      label: Steps to reproduce
      description: We highly suggest including screenshots and a bug report log. Please use the right markdown syntax for code blocks.
      placeholder: Having detailed steps helps us reproduce the bug. If you have logs, please use fenced code blocks (triple backticks ```) to format them.
    validations:
      required: true

  - type: textarea
    attributes:
      label: ✔️ Expected Behavior
      description: Describe what you expected to happen.
      placeholder: What were you expecting? Please do not copy and paste the steps to reproduce here.
    validations:
      required: true

  - type: textarea
    attributes:
      label: ❌ Actual Behavior
      description: Describe what actually happened.
      placeholder: What happened instead? Please do not copy and paste the steps to reproduce here.
    validations:
      required: false