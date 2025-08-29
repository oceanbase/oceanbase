---
name: 🙋 Question
about: I have a question.
title: "[Question]: "
labels: ["type: question"]
---

name: "🙋 Question"
description: Ask a question about OceanBase\mlabels:
  - type: question
body:
  - type: checkboxes
    attributes:
      label: Self Checks
      description: "To make sure we get to you in time, please check the following :)"
      options:
        - label: I have read the [Contributing Guide](https://github.com/oceanbase/oceanbase/blob/develop/CONTRIBUTING.md).
          required: true
        - label: This is only for question, if you would like to report a bug, please use the bug report template.
          required: true
        - label: I have searched for existing issues [search for existing issues](https://github.com/oceanbase/oceanbase/issues), including closed ones.
          required: true
        - label: I confirm that I am using English to submit this question, otherwise it will be closed.
          required: true
        - label: 【中文用户 & Non English User】请使用英语提交，否则会被关闭 ：）
          required: true
        - label: "Please do not modify this template :) and fill in all the required fields."
          required: true

  - type: textarea
    attributes:
      label: Quick Question (Optional)
      description: If your question is concise and probably has a short answer, you may also ask it in our [Discord Community](https://discord.gg/74cF8vbNEs) for faster response.
    validations:
      required: false

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
      label: Your Question
      description: Please describe your question in detail.
      placeholder: What would you like to know about OceanBase? Please provide as much context as possible.
    validations:
      required: true

  - type: textarea
    attributes:
      label: Relevant Context
      description: Add any other context or screenshots about your question here.
      placeholder: Include any code snippets, logs, or other information that might help us understand your question.
    validations:
      required: false

  - type: textarea
    attributes:
      label: What You've Tried
      description: Describe what you have tried so far to find an answer to your question.
      placeholder: Have you checked the documentation, searched for similar issues, or tried any solutions?
    validations:
      required: false
