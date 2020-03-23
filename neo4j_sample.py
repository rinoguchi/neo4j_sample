import os
from pathlib import Path
from neo4j import GraphDatabase, Driver
from neo4j_client import Neo4jServer, Neo4jClient


def test_load_csv():
    def __test_LOAD_CSV():
        driver: Driver = GraphDatabase.driver('bolt://localhost:7687', auth=('neo4j', 'password'), encrypted=False)
        with driver.session() as session:
            session.run('MATCH (x) DETACH DELETE x')

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

            session.run('MATCH (x) DETACH DELETE x')

    def __test_apoc_load_csv():
        driver: Driver = GraphDatabase.driver('bolt://localhost:7687', auth=('neo4j', 'password'), encrypted=False)
        with driver.session() as session:
            session.run('MATCH (x) DETACH DELETE x')

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

            session.run('MATCH (x) DETACH DELETE x')

    def __test_apoc_import_csv():
        driver: Driver = GraphDatabase.driver('bolt://localhost:7687', auth=('neo4j', 'password'), encrypted=False)
        with driver.session() as session:
            session.run('MATCH (x) DETACH DELETE x')

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

            session.run('MATCH (x) DETACH DELETE x')

    # メイン
    with Neo4jServer():  # Neo4j立ち上げ
        __test_LOAD_CSV()
        __test_apoc_load_csv()
        __test_apoc_import_csv()


def test_admin_import_csv():
    flag_path: str = './import/admin_import/do_import_flag'
    Path(flag_path).touch()
    with Neo4jServer():  # Neo4jを立ち上げて、import_csv.shを実行
        driver: Driver = GraphDatabase.driver('bolt://localhost:7687', auth=('neo4j', 'password'), encrypted=False)
        with driver.session() as session:
            assert session.run('MATCH (n:Node) RETURN count(n) as cnt').single()['cnt'] == 9
            assert session.run('MATCH ()-[l:LINKED]->() RETURN count(l) as cnt').single()['cnt'] == 6

            session.run('MATCH (x) DETACH DELETE x')

    os.remove(flag_path)


if __name__ == '__main__':
    test_load_csv()
    test_admin_import_csv()
