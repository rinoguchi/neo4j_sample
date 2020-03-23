# Neo4jにアクセスするためのクライアントを提供します。

from neo4j import GraphDatabase, Driver, StatementResult, TransactionError, Session, Transaction
from neobolt.exceptions import ServiceUnavailable
from typing import Dict, Any, Tuple
import traceback
import time
import subprocess


class Neo4jServer:

    def __enter__(self):
        """
        dockerコンテナを起動する
        """
        subprocess.call(['docker-compose', 'up', '-d'])
        while self.__neo4j_available() is False:
            time.sleep(10)
        return self

    def __exit__(self, exc_type, exc_value, tb):
        """
        dockerコンテナを停止する
        """
        if tb is not None:
            print(''.join(traceback.format_tb(tb)))
        subprocess.call(['docker-compose', 'stop'])

    def __neo4j_available(self) -> bool:
        """
        Neo4jが利用可能かどうかをチェックする
        """
        try:
            GraphDatabase.driver('bolt://localhost:7687', auth=('neo4j', 'password'), encrypted=False)
        except ServiceUnavailable:
            print('neo4j is not available yet.')
            return False
        print('neo4j is already available.')
        return True


class Neo4jClient:
    """
    Neo4jにアクセスするためのクライアントモジュール
    使い方:
        # 参照
        with Neo4jClient() as client:
            for row in client.run('MATCH (t:Test) WHERE t.name = $name RETURN t.id as id', name='hoge')
                print(row[id])

        # 更新
        with Neo4jClient(readonly=False) as client:
            client.run('MATCH (t:Test) DETACH DELETE t')
            client.run('CREATE (:Test {id:1})')
            client.run('CREATE (:Test {id:2})')
            if bussiness_check_ok():
                client.commit()
            else:
                client.rollback()
    """
    driver: Driver
    session: Session
    transaction: Transaction
    readonly: bool
    with_transaction: bool

    def __init__(
            self,
            uri: str = 'bolt://localhost:7687',
            auth: Tuple[str, str] = ('neo4j', 'admin'),
            readonly: bool = True,
            with_transaction: bool = True):
        self.driver = GraphDatabase.driver(uri, auth=auth, encrypted=False)
        self.session = self.driver.session()
        self.readonly = readonly
        self.with_transaction = with_transaction
        self.transaction = self.session.begin_transaction() if self.with_transaction else None

    def commit(self):
        if self.readonly:
            raise TransactionError('cannot commit when readonly')
        if self.session.has_transaction():
            self.transaction.commit()
            print('neo4j transaction commited.')

    def rollback(self):
        if self.session.has_transaction():
            self.transaction.rollback()
            print('neo4j transaction rollbacked.')

    def run(self, query: str, **kwargs: Dict[str, Any]) -> StatementResult:
        print(f'neo4j query: {query}')
        if len(kwargs) > 0:
            print(f'neo4j kwargs: {kwargs}')
        result: StatementResult
        if self.with_transaction:
            result = self.transaction.run(query, kwargs)
        else:
            result = self.session.run(query, kwargs)
        return result

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, tb):
        if tb is not None:
            print(''.join(traceback.format_tb(tb)))
        if self.session.has_transaction():
            self.rollback()
        self.session.close()
        self.driver.close()
