<p align="center">
    <a href="https://github.com/oceanbase/oceanbase">
        <img alt="OceanBase Logo" src="images/logo.svg" width="50%" />
    </a>
</p>

<p align="center">
    <a href="https://en.oceanbase.com/docs/oceanbase-database">
        <img alt="English doc" src="https://img.shields.io/badge/docs-English-blue" />
    </a>
    <a href="https://www.oceanbase.com/docs/oceanbase-database-cn">
        <img alt="Chinese doc" src="https://img.shields.io/badge/文档-简体中文-blue" />
    </a>
    <a href="https://github.com/oceanbase/oceanbase/commits/master">
        <img alt="last commit" src="https://img.shields.io/github/last-commit/oceanbase/oceanbase/master" />
    </a>
    <a href="https://github.com/oceanbase/oceanbase">
        <img alt="stars" src="https://img.shields.io/badge/dynamic/json?color=blue&label=stars&query=stargazers_count&url=https%3A%2F%2Fapi.github.com%2Frepos%2Foceanbase%2Foceanbase" />
    </a>
    <a href="https://github.com/oceanbase/oceanbase/actions/workflows/compile.yml">
        <img alt="building status" src="https://img.shields.io/github/actions/workflow/status/oceanbase/oceanbase/compile.yml?branch=master" />
    </a>
    <a href="https://github.com/oceanbase/oceanbase/blob/master/LICENSE">
        <img alt="license" src="https://img.shields.io/badge/license-MulanPubL--2.0-blue" />
    </a>
</p>

<p align="center">
    <a href="https://discord.gg/74cF8vbNEs">
        <img alt="Support" src="https://img.shields.io/badge/Disord-Join%20Oceanbase-brightgreen?logo=discord" />
    </a>
    <a href="https://stackoverflow.com/questions/tagged/oceanbase">
        <img alt="Stack Overflow" src="https://img.shields.io/badge/Stack-Stack%20Overflow-brightgreen?logo=stackoverflow" />
    </a>
    <a href="https://www.youtube.com/@OceanBaseDB">
        <img alt="Static Badge" src="https://img.shields.io/badge/YouTube-red?logo=youtube">
    </a>
    <a href="https://www.linkedin.com/company/oceanbase" target="_blank">
        <img src="https://custom-icon-badges.demolab.com/badge/LinkedIn-0A66C2?logo=linkedin-white&logoColor=fff" alt="follow on LinkedIn">
    </a>
    <a href="https://en.oceanbase.com/">
        <img alt="Static Badge" src="https://img.shields.io/badge/OceanBase-Official%20Website-blue">
    </a>
    <a href="https://oceanbase.github.io/docs/blogs/users/1st-financial">
        <img alt="Static Badge" src="https://img.shields.io/badge/OceanBase-Official%20Blog-blue">
    </a>
    <a href="https://x.com/OceanBaseDB">
        <img alt="Static Badge" src="https://img.shields.io/badge/Follow-%40OceanBaseDB-white?logo=x&labelColor=black">
    </a>
    <a href="https://github.com/oceanbase/oceanbase/graphs/commit-activity" target="_blank">
        <img alt="Static Badge" src="https://img.shields.io/badge/commit%20activity%201020%2Fmonth-yellow">
    </a>
</p>

[English](README.md) | [中文版](README_CN.md) | 日本語版

**OceanBase** は分散リレーショナルデータベースです。Ant Group によって開発されています。OceanBase は、汎用的なサーバークラスター上に構築されています。 OceanBase は、[Paxos](https://lamport.azurewebsites.net/pubs/lamport-paxos.pdf) プロトコルとその分散構造に基づいて、高可用性と線形スケーラビリティを実現します。OceanBase は、特定のハードウェア アーキテクチャに依存しません。また、ベクトルデータベース機能をサポートしており、AI や大規模検索シナリオ向けに高効率なベクトル検索能力を提供します。

# 主な特徴

- **ベクトル検索**: ベクトルインデックスと高効率なクエリをサポートし、AI アプリケーション、レコメンデーションシステム、セマンティック検索に適しています。高スループットかつ低遅延のベクトル検索機能を提供します。
- **透過的なスケーラビリティ**: 1 つのクラスターに 1,500 ノード、PB データ、1 兆行のレコード。
- **超高速パフォーマンス**: TPC-C 7 億 700 万 tmpC、TPC-H 1526 万 QphH @30000GB。
- **コスト効率**: ストレージ コストを 70%～90% 削減。
- **リアルタイム分析**: 追加コストなしで HTAP をサポート。
- **継続的可用性**: RPO = 0 (データ損失ゼロ)、RTO < 8 秒 (リカバリ時間)
- **MySQL 互換**: MySQL から簡単に移行できます。

詳細については、[主な機能](https://en.oceanbase.com/product/opensource)も参照してください。

# クイック スタート

詳細については、[クイック エクスペリエンス](https://en.oceanbase.com/docs/community-observer-en-10000000000829647)

## 🔥 オールインワンな環境から始める

次のコマンドを使用すると、スタンドアロンの OceanBase をすぐにデプロイして試すことができます。

**注記**: Linuxのみ

```shell
# オールインワンパッケージをダウンロードしてインストールします（インターネット接続が必要です）
bash -c "$(curl -s https://obbusiness-private.oss-cn-shanghai.aliyuncs.com/download-center/opensource/oceanbase-all-in-one/installer.sh)"
source ~/.oceanbase-all-in-one/bin/env.sh

# OceanBaseデータベースを迅速に導入
obd demo
```

## 🐳 Dockerで始める

**注記**: [dockerhub](https://hub.docker.com/r/oceanbase/oceanbase-ce/tags)、[quay.io](https://quay.io/repository/oceanbase/oceanbase-ce?tab=tags)、[ghcr.io](https://github.com/oceanbase/docker-images/pkgs/container/oceanbase-ce) でイメージを提供しています。dockerhub からイメージを取得できない場合は、他の 2 つのレジストリを試してください。

1. OceanBase インスタンスを起動します。

    ```shell
    # ミニ・スタンドアロンインスタンスをデプロイします。
    docker run -p 2881:2881 --name oceanbase-ce -e MODE=mini -d oceanbase/oceanbase-ce

    # quay.io のイメージを使用してミニ・スタ​​ンドアロンインスタンスをデプロイします。
    # docker run -p 2881:2881 --name oceanbase-ce -e MODE=mini -d quay.io/oceanbase/oceanbase-ce

    # ghcr.io のイメージを使用してミニ・スタ​​ンドアロンインスタンスをデプロイします。
    # docker run -p 2881:2881 --name oceanbase-ce -e MODE=mini -d ghcr.io/oceanbase/oceanbase-ce
    ```

2. OceanBase インスタンスに接続します。

    ```shell
    docker exec -it oceanbase-ce obclient -h127.0.0.1 -P2881 -uroot # sys テナントの root ユーザーに接続します。
    ```

詳細については、[Docker Readme](https://github.com/oceanbase/docker-images/blob/main/oceanbase-ce/README.md)も参照してください。

## ☸️ Kubernetesから始める

[ob-operator](https://github.com/oceanbase/ob-operator)を使用すると、OceanBase DatabaseインスタンスをKubernetesクラスターに素早くデプロイして管理できます。詳細については、[ob-operatorのクイックスタート](https://oceanbase.github.io/ob-operator)のドキュメントを参照してください。

## 👨‍💻 開発を始める
手動でオブザーバーをコンパイルしてデプロイする方法については、[OceanBase 開発者ドキュメント](https://oceanbase.github.io/oceanbase/build-and-run)を参照してください。

# ロードマップ

今後の計画については、[製品のイテレーションの進捗状況](https://github.com/oceanbase/oceanbase/issues/1839)を参照してください。詳細については、[OceanBase ロードマップ](https://github.com/orgs/oceanbase/projects/4)も参照してください。

# ケーススタディ

OceanBase は 2,000 社を超える顧客にサービスを提供しており、金融サービス、通信、小売、インターネットなど、さまざまな業界のデータベースをアップグレードしてきました。

詳細については、[成功事例](https://en.oceanbase.com/customer/home) と [OceanBase のユーザー](https://github.com/oceanbase/oceanbase/issues/1301) もご覧ください。

# システムアーキテクチャ

[システムアーキテクチャの概要](https://en.oceanbase.com/docs/community-observer-en-10000000000829641)

# 貢献する

貢献を心より歓迎します。開始するには、[開発ガイド](https://oceanbase.github.io/oceanbase)をお読みください。

# ライセンス

OceanBase は、Mulan Public License バージョン 2 に基づいてライセンスされています。詳細については、[LICENSE](LICENSE) ファイルを参照してください。

# コミュニティ

OceanBase コミュニティにぜひご参加ください！

* [Discord](https://discord.gg/74cF8vbNEs): ご質問の投稿、フィードバックの共有、最新情報の取得、および他のOceanBaseユーザーとの交流が可能です。
* [GitHub Issues](https://github.com/oceanbase/oceanbase/issues): ご報告いただける内容には、OceanBaseのご利用中に遭遇した不具合や、新たな機能に関するご要望が含まれます。
