# neo4j_sample

## 環境構築

1. 以下に従い、python3.7とvirtualenvを事前intall
   https://cloud.google.com/python/setup?hl=ja

1. git clone
   ```sh
   git clone https://github.com/rinoguchi/neo4j_sample.git
   cd neo4j_sample
   ```

1. virtualenvでプロジェクト内に仮想環境を構築
   c++環境構築後に実行する必要があるので注意が必要。
   ```sh
   virtualenv --python python3 env
   # python3のところは、3.7系のコマンド名を記載する
   ```

1. 仮想環境を有効化
   ```sh
   source env/bin/activate
   ```

1. 依存関係を解決
   ```sh
   pip install -r requirements.txt
   ```

1. 仮想環境を無効化
   ```sh
   deactivate
   ```

## Neo4jの手動起動
```sh
# 起動
docker-compose up -d

# ログ確認
docker-compose logs -f

# 停止
docker-compose stop
```

## サンプルプログラム実行
```sh
python neo4j_sample.py
```
