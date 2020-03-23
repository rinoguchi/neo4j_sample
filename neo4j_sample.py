import os
import sys
from pathlib import Path
from neo4j import GraphDatabase, Driver, TransactionError
from neo4j_client import Neo4jServer, Neo4jClient


def test_neo4j_client():

    def __reset():
        with Neo4jClient(with_transaction=False) as client:
            client.run('MATCH (x) DETACH DELETE x')

    def __test_rollback():
        with Neo4jClient(readonly=False) as client:
            client.run('CREATE (:Test {id:1})')
            client.run('CREATE (:Test {id:2})')
            assert {1, 2} == {row['id'] for row in client.run('MATCH (t:Test) RETURN t.id as id')}

        with Neo4jClient() as client:
            assert [] == [row['id'] for row in client.run('MATCH (t:Test) RETURN t.id as id')]
        print(f'{sys._getframe().f_code.co_name} succeeded.')

    def __test_rollback_explicitly():
        with Neo4jClient(readonly=False) as client:
            client.run('CREATE (:Test {id:1})')
            client.run('CREATE (:Test {id:2})')
            assert {1, 2} == {row['id'] for row in client.run('MATCH (t:Test) RETURN t.id as id')}
            client.rollback()

        with Neo4jClient() as client:
            assert [] == [row['id'] for row in client.run('MATCH (t:Test) RETURN t.id as id')]
        print(f'{sys._getframe().f_code.co_name} succeeded.')

    def __test_commit():
        with Neo4jClient(readonly=False) as client:
            client.run('MATCH (t:Test) DETACH DELETE t')
            client.run('CREATE (:Test {id:101})')
            client.run('CREATE (:Test {id:102})')
            assert {101, 102} == {row['id'] for row in client.run('MATCH (t:Test) RETURN t.id as id')}
            client.commit()

        with Neo4jClient() as client:
            assert {101, 102} == {row['id'] for row in client.run('MATCH (t:Test) RETURN t.id as id')}
        __reset()
        print(f'{sys._getframe().f_code.co_name} succeeded.')

    def __test_commit_when_readonly():
        with Neo4jClient() as client:
            client.run('CREATE (:Test {id:101})')
            client.run('CREATE (:Test {id:102})')
            assert {101, 102} == {row['id'] for row in client.run('MATCH (t:Test) RETURN t.id as id')}
            try:
                client.commit()
            except BaseException as e:
                assert isinstance(e, TransactionError)
        print(f'{sys._getframe().f_code.co_name} succeeded.')

    # メイン
    with Neo4jServer():  # Neo4j立ち上げ
        __reset()
        __test_rollback()
        __test_rollback_explicitly()
        __test_commit()
        __test_commit_when_readonly()
        __reset()


def test_load_csv():
    def __reset():
        with Neo4jClient(with_transaction=False) as client:
            client.run('MATCH (x) DETACH DELETE x')

    def __test_LOAD_CSV():
        driver: Driver = GraphDatabase.driver('bolt://localhost:7687', auth=('neo4j', 'password'), encrypted=False)
        with driver.session() as session:

            # ノード用CSVデータをロード
            nodes_query: str = """
                USING PERIODIC COMMIT 10000
                LOAD CSV WITH HEADERS FROM $csv_path AS line
                CREATE (:Node {id:toInteger(line.id)})
            """
            session.run(nodes_query, csv_path='file:///load_csv/nodes.csv')
            assert session.run('MATCH (n:Node) RETURN count(n) AS cnt').single()['cnt'] == 9

            # リレーション用CSVデータをロード
            relations_query: str = """
                USING PERIODIC COMMIT 10000
                LOAD CSV WITH HEADERS FROM $csv_path AS line
                MATCH (nf:Node {id:toInteger(line.from_id)}), (nt:Node {id:toInteger(line.to_id)})
                CREATE (nf)-[:LINKED {score: toFloat(line.score)}]->(nt)
            """
            session.run(relations_query, csv_path='file:///load_csv/relations.csv')
            assert session.run('MATCH ()-[l:LINKED]->() RETURN count(l) AS cnt').single()['cnt'] == 6
        print(f'{sys._getframe().f_code.co_name} succeeded.')

    def __test_apoc_load_csv():
        driver: Driver = GraphDatabase.driver('bolt://localhost:7687', auth=('neo4j', 'password'), encrypted=False)
        with driver.session() as session:
            nodes_query: str = """
                CALL apoc.load.csv($csv_path) YIELD map AS line
                CREATE (:Node {id:toInteger(line.id)})
            """
            session.run(nodes_query, csv_path='file:///apoc_load_csv/nodes.csv')
            assert session.run('MATCH (n:Node) RETURN count(n) AS cnt').single()['cnt'] == 9

            # リレーション用CSVデータをロード
            relations_query: str = """
                CALL apoc.load.csv($csv_path) yield map AS line
                MATCH (nf:Node {id:toInteger(line.from_id)}), (nt:Node {id:toInteger(line.to_id)})
                CREATE (nf)-[:LINKED {score: toFloat(line.score)}]->(nt)
            """
            session.run(relations_query, csv_path='file:///apoc_load_csv/relations.csv')
            assert session.run('MATCH ()-[l:LINKED]->() RETURN count(l) AS cnt').single()['cnt'] == 6

            # リレーション用CSVデータをロード（バッチインサート版）
            relations_batch_query: str = """
                CALL apoc.periodic.iterate('
                    CALL apoc.load.csv($path) yield map as line
                ', '
                    MATCH (nf:Node {id:toInteger(line.from_id)}), (nt:Node {id:toInteger(line.to_id)})
                    CREATE (nf)-[:LINKED {score: toFloat(line.score)}]->(nt)
                ', {batchSize:10000, iterateList:true, parallel:true, params:{path:$csv_path}});
            """
            session.run(relations_batch_query, csv_path='file:///apoc_load_csv/relations.csv')
            assert session.run('MATCH ()-[l:LINKED]->() RETURN count(l) AS cnt').single()['cnt'] == 12
        print(f'{sys._getframe().f_code.co_name} succeeded.')

    def __test_apoc_import_csv():
        driver: Driver = GraphDatabase.driver('bolt://localhost:7687', auth=('neo4j', 'password'), encrypted=False)
        with driver.session() as session:
            query: str = """
                CALL apoc.import.csv(
                    [{fileName: $node_csv_path, labels: ['Node']}],
                    [{fileName: $relation_csv_path, type: 'LINKED'}],
                    {}
                )
            """
            session.run(query, node_csv_path=f'file:///apoc_import_csv/nodes.csv', relation_csv_path=f'file:///apoc_import_csv/relations.csv')
            assert session.run('MATCH (n:Node) RETURN count(n) AS cnt').single()['cnt'] == 9
            assert session.run('MATCH ()-[l:LINKED]->() RETURN count(l) AS cnt').single()['cnt'] == 6
        print(f'{sys._getframe().f_code.co_name} succeeded.')

    # メイン
    with Neo4jServer():  # Neo4j立ち上げ
        __reset()
        __test_LOAD_CSV()
        __reset()
        __test_apoc_load_csv()
        __reset()
        __test_apoc_import_csv()
        __reset()


def test_admin_import_csv():
    flag_file_path: str = './import/admin_import/do_import_flag'
    Path(flag_file_path).touch()
    with Neo4jServer():  # Neo4jを立ち上げて、import_csv.shを実行
        driver: Driver = GraphDatabase.driver('bolt://localhost:7687', auth=('neo4j', 'password'), encrypted=False)
        with driver.session() as session:
            assert session.run('MATCH (n:Node) RETURN count(n) as cnt').single()['cnt'] == 9
            assert session.run('MATCH ()-[l:LINKED]->() RETURN count(l) as cnt').single()['cnt'] == 6

            session.run('MATCH (x) DETACH DELETE x')

    os.remove(flag_file_path)
    print(f'{sys._getframe().f_code.co_name} succeeded.')


if __name__ == '__main__':
    test_neo4j_client
    test_load_csv()
    test_admin_import_csv()
