<p align="center">
    <a href="https://github.com/oceanbase/oceanbase">
        <img alt="OceanBase Logo" src="images/logo.svg" width="50%" />
    </a>
</p>

<p align="center">
    <a href="https://github.com/oceanbase/oceanbase/blob/master/LICENSE">
        <img alt="license" src="https://img.shields.io/badge/license-MulanPubL--2.0-blue" />
    </a>
    <a href="https://github.com/oceanbase/oceanbase/releases/latest">
        <img alt="license" src="https://img.shields.io/badge/dynamic/json?color=blue&label=release&query=tag_name&url=https%3A%2F%2Fapi.github.com%2Frepos%2Foceanbase%2Foceanbase%2Freleases%2Flatest" />
    </a>
    <a href="https://github.com/oceanbase/oceanbase">
        <img alt="stars" src="https://img.shields.io/badge/dynamic/json?color=blue&label=stars&query=stargazers_count&url=https%3A%2F%2Fapi.github.com%2Frepos%2Foceanbase%2Foceanbase" />
    </a>
    <a href="https://github.com/oceanbase/oceanbase">
        <img alt="forks" src="https://img.shields.io/badge/dynamic/json?color=blue&label=forks&query=forks&url=https%3A%2F%2Fapi.github.com%2Frepos%2Foceanbase%2Foceanbase" />
    </a>
    <a href="https://en.oceanbase.com/docs/oceanbase-database">
        <img alt="English doc" src="https://img.shields.io/badge/docs-English-blue" />
    </a>
    <a href="https://www.oceanbase.com/docs/oceanbase-database-cn">
        <img alt="Chinese doc" src="https://img.shields.io/badge/文档-简体中文-blue" />
    </a>
    <a href="https://github.com/oceanbase/oceanbase/actions/workflows/compile.yml">
        <img alt="building status" src="https://img.shields.io/github/actions/workflow/status/oceanbase/oceanbase/compile.yml?branch=master" />
    </a>
    <a href="https://github.com/oceanbase/oceanbase/commits/master">
        <img alt="last commit" src="https://img.shields.io/github/last-commit/oceanbase/oceanbase/master" />
    </a>
    <a href="https://join.slack.com/t/oceanbase/shared_invite/zt-1e25oz3ol-lJ6YNqPHaKwY_mhhioyEuw">
        <img alt="Join Slack" src="https://img.shields.io/badge/slack-Join%20Oceanbase-brightgreen?logo=slack" />
    </a>
</p>

[English](README.md) | [中文版](README_CN.md) | 日本語

**OceanBase Database** は、分散リレーショナルデータベースです。すべて Ant グループによって開発されています。OceanBase Database は共通のサーバクラスタ上に構築されています。[Paxos](https://lamport.azurewebsites.net/pubs/lamport-paxos.pdf) プロトコルとその分散構造に基づき、OceanBase Database は高い可用性と線形スケーラビリティを提供します。OceanBase Database は特定のハードウェアアーキテクチャに依存しません。

# 主な特徴

- **透過的なスケーラビリティ**

    OceanBase クラスタは 1,500 ノードまで透過的に拡張でき、ペタバイトのデータと 1 兆行のレコードを扱うことができます。

- **超高速パフォーマンス**

    7 億 700 万 tmpC の TPC-C 記録と1,526 万 QphH@30000GB の TPC-H 記録の両方を更新した唯一の分散型データベース。

- **リアルタイムのオペレーション分析**

    トランザクションとリアルタイムの業務分析ワークロードの両方に対応する統合システム。

- **継続的な可用性**

    OceanBase Database は Paxos Consensus アルゴリズムを採用し、RPO ゼロ、RTO8 秒以下を実現。都市内/遠隔地のディザスタリカバリをサポートし、複数拠点でのマルチアクティビティとデータロスをゼロにします。

- **MySQL 対応**

    OceanBase Database は MySQL と高い互換性があるため、移行に必要な修正はゼロかわずかです。

- **コストパフォーマンス**

    最先端の圧縮技術により、パフォーマンスを損なうことなく、ストレージコストを 70％～90％ 削減。マルチテナンシーアーキテクチャにより、より高いリソース利用率を実現します。

詳しくは[主な特徴](https://en.oceanbase.com/product/opensource)も参照のこと。

# クイックスタート

## 🔥 オールインワンで始める

以下のコマンドで、スタンドアロンのOceanBase Databaseを素早くデプロイして体験することができます。

**注**: Linux のみ

```shell
# オールインワンパッケージをダウンロードしてインストールする（インターネット接続が必要です）
bash -c "$(curl -s https://obbusiness-private.oss-cn-shanghai.aliyuncs.com/download-center/opensource/oceanbase-all-in-one/installer.sh)"
source ~/.oceanbase-all-in-one/bin/env.sh

# OceanBase Database の迅速な展開
obd demo
```

## 🐳 Docker で始める

1. OceanBase Database インスタンスを起動する:

    ```shell
    # Deploy a mini standalone instance.
    docker run -p 2881:2881 --name oceanbase-ce -e MINI_MODE=1 -d oceanbase/oceanbase-ce
    ```

2. OceanBase Database インスタンスに接続する:

    ```shell
    docker exec -it oceanbase-ce ob-mysql sys # sys tenant の root ユーザーに接続する。
    ```

詳しくは [クイックエクスペリエンス](https://en.oceanbase.com/docs/community-observer-en-10000000000829647) または [クイックスタート (簡体中国語)](https://open.oceanbase.com/quickStart)もご参照ください。

## 👨‍💻 開発を開始する

手動でコンパイルしたオブザーバをコンパイルしてデプロイする方法については、[OceanBase 開発ドキュメント](https://github.com/oceanbase/oceanbase/wiki/Compile)を参照してください。

# ロードマップ

今後の計画については[ロードマップ 2023](https://github.com/oceanbase/oceanbase/issues/1364) を参照。詳細は [OceanBase ロードマップ](https://github.com/orgs/oceanbase/projects)も参照。

# ケーススタディ

OceanBase は 400 社以上のお客様にサービスを提供し、金融、通信、小売、インターネットなど様々な業界のデータベースをアップグレードしてきました。

詳しくは[成功事例](https://en.oceanbase.com/customer/home)や [OceanBase を使っている人](https://github.com/oceanbase/oceanbase/issues/1301)もご覧ください。

# システムアーキテクチャ

[システムアーキテクチャの紹介](https://en.oceanbase.com/docs/community-observer-en-10000000000829641)

# コントリビュート

コントリビュートは大いに歓迎されます。まずは[開発ガイド](docs/README.md)をお読みください。

# ライセンス

OceanBase Database は Mulan Public License, Version 2 の下でライセンスされています。詳細は [LICENSE](LICENSE) ファイルを参照してください。

# コミュニティ

OceanBase のコミュニティに参加する:

* [Slack ワークスペース](https://join.slack.com/t/oceanbase/shared_invite/zt-1e25oz3ol-lJ6YNqPHaKwY_mhhioyEuw)
* [中国ユーザーフォーラム](https://ask.oceanbase.com/)
* DingTalk グループ: 33254054 ([QR コード](images/dingtalk.svg))
* WeChat グループ (WeChat ID でアシスタントを追加する: OBCE666)
