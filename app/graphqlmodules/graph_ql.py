from __future__ import annotations
import neo4j

import strawberry

from strawberry.fastapi import GraphQLRouter, BaseContext
from strawberry.types import Info
from typing import Union, Optional
from fastapi import Depends
from neo4j import GraphDatabase, basic_auth
from enum import Enum

from ..dependencies import custom_context_dependency, CustomContext

# resolvers
def driver_meta_resolver(root: ControllerMeta, info: Info,
                    id: Optional[int] = None, name: Optional[str] = None, model: Optional[str] = None, 
                    vendor: Optional[str] = None, capacity: Optional[float] = None, 
                    linked_id: Optional[int] = None
                    ) -> list[Union[DriverMeta, None]]:
        def cypher_query_driver_meta(tx: neo4j.Transaction, id, name, model, vendor, capacity):
            if linked_id or root:
                relation_id = linked_id if linked_id else root.id
                results = tx.run(
                    f"""
                    MATCH (m:`{Label.DRIVER_META.value}`)-[r]-(n) 
                    WHERE (id(n) = $linked_id)
                    AND (id(m) = $id or $id is null)
                    AND (m.name CONTAINS $name or $name is null)
                    AND (m.model CONTAINS $model or $model is null)
                    AND (m.vendor CONTAINS $vendor or $vendor is null)
                    AND (m.capacity = $capacity or $capacity is null)
                    AND (id(n) = $linked_id or $linked_id is null)
                    RETURN id(m) as id, m
                    """,
                    id=id, name=name, model=model, vendor=vendor, capacity=capacity, linked_id=relation_id
                )
            else:
                results = tx.run(
                    f"""
                    MATCH (m:`{Label.DRIVER_META.value}`) 
                    WHERE (id(m) = $id or $id is null)
                    AND (m.name CONTAINS $name or $name is null)
                    AND (m.model CONTAINS $model or $model is null)
                    AND (m.vendor CONTAINS $vendor or $vendor is null)
                    AND (m.capacity = $capacity or $capacity is null)
                    RETURN id(m) as id, m
                    """,
                    id=id, name=name, model=model, vendor=vendor, capacity=capacity
                )
            return list(results)

        c_contex: CustomContext = info.context

        with c_contex.driver.session(database="neo4j") as session:
            results = session.execute_read(cypher_query_driver_meta, id=id, name=name, model=model, vendor=vendor, capacity=capacity)
            resolver_result = [DriverMeta(id=result["id"], **result['m']) for result in results]
            return resolver_result


def controller_meta_resolver(root: ControllerMeta, info: Info, 
                    id: Optional [int] = None, model: Optional[str] = None, vendor: Optional[str] = None,
                    description: Optional[str] = None, created_at: Optional[int] = None,
                    linked_id: Optional[int] = None
                    ) -> list[Union[ControllerMeta, None]]:
    def cypher_query_controller_meta(tx: neo4j.Transaction, id, model, vendor, description, created_at):
        if linked_id or root:
            relation_id = linked_id if linked_id else root.id
            results = tx.run(
                f"""
                MATCH (m:`{Label.CONTROLLER_META.value}`)-[r]-(n)
                WHERE (id(n) = $linked_id )
                AND (id(m) = $id or $id is null)
                AND (m.model CONTAINS $model or $model is null)
                AND (m.vendor CONTAINS $vendor or $vendor is null)
                AND (m.description CONTAINS $description or $description is null)
                AND (m.created_at = $created_at or $created_at is null)
                RETURN id(m) as id ,m
                """,
                id=id, model=model, vendor=vendor, description=description, created_at=created_at, linked_id=relation_id
            )
        else :
            results = tx.run(
                f"""
                MATCH (m:`{Label.CONTROLLER_META.value}`) 
                WHERE (id(m) = $id or $id is null)
                AND (m.model CONTAINS $model or $model is null)
                AND (m.vendor CONTAINS $vendor or $vendor is null)
                AND (m.description CONTAINS $description or $description is null)
                AND (m.created_at = $created_at or $created_at is null)
                RETURN id(m) as id ,m
                """,
                id=id, model=model, vendor=vendor, description=description, created_at=created_at
            )
        return list(results)

    c_contex: CustomContext = info.context
    with c_contex.driver.session(database="neo4j") as session:
        results = session.execute_read(cypher_query_controller_meta, 
                                        id, model, vendor, description, created_at)
        return [ControllerMeta(id=result["id"], model=result["m"]["model"], vendor=result["m"]["vendor"], 
                                description=result["m"]["description"], created_at=result["m"]["created_at"]) for result in results]

def asssd_benchmark_resolver(root: ControllerMeta, info: Info,
                    id: Optional[int] = None, name: Optional[str] = None,
                    ) -> list[Union[ASSSDBenchMark, None]]:
    def cypher_query_asssd_benchmark(tx: neo4j.Transaction, id, name):
        results = tx.run(
            f"""
            MATCH (m:`{Label.ASSSD_BENCHMARK.value}`)-[r]-(n)
            WHERE (id(n) = $linked_id)
            AND (id(m) = $id or $id is null)
            AND (m.name CONTAINS $name or $name is null)
            RETURN id(m) as id, m
            """,
            id=id, name=name, linked_id=root.id
        )
        return list(results)
    
    c_contex: CustomContext = info.context
    with c_contex.driver.session(database="neo4j") as session:
        results = session.execute_read(cypher_query_asssd_benchmark, id=id, name=name)
        return [ASSSDBenchMark(id=result["id"], name=result["m"]["name"], description=result["m"]['description']) for result in results]


def record_value_resolver(root: ASSSDBenchMark, info: Info,
                    id: Optional[int] = None, type: Optional[RecordType] = None,
                    unit: Optional[RecordUnit] = None, name: Optional[RecordName] = None,
                    value: Optional[float] = None, created_at: Optional[int] = None,
                    ) -> list[Union[RecordValue, None]]:
    def cypher_query_record_value(tx: neo4j.Transaction, id, type, unit, name, value, created_at):
        results = tx.run(
            f"""
            MATCH (m:`{Label.RECORD_VALUE.value}`)-[r]-(n)
            WHERE (id(n) = $linked_id)
            AND (id(m) = $id or $id is null)
            AND (m.type = $record_type or $record_type is null)
            AND (m.unit = $record_unit or $record_unit is null)
            AND (m.name = $record_name or $record_name is null)
            AND (m.value = $value or $value is null)
            AND (m.created_at = $created_at or $created_at is null)
            RETURN id(m) as id, m
            """,
            id=id, record_type=type, record_unit=unit, record_name=name, value=value, created_at=created_at, linked_id=root.id
        )
        return list(results)
    
    c_contex: CustomContext = info.context
    with c_contex.driver.session(database="neo4j") as session:
        results = session.execute_read(cypher_query_record_value, id=id, type=type, unit=unit, name=name, value=value, created_at=created_at)
        return [RecordValue(id=result["id"], type=RecordType(result["m"]["type"]), unit=RecordUnit(result["m"]["unit"]), 
                            name=RecordName(result["m"]["name"]), value=result["m"]["value"], created_at=result["m"]["created_at"]) for result in results]
    



# 枚举类
@strawberry.enum
class RecordType(Enum):
    READ = "READ"
    WRITE = "WRITE"
    MIX = "MIX"

@strawberry.enum
class RecordUnit(Enum):
    IOPS = "IOPS"
    MBPS = "MBPS"
    LATENCY = "LATENCY"

@strawberry.enum
class RecordName(Enum):
    SEQ = "SEQ"
    RAND = "RAND"
    SEQ_LATENCY = "SEQ_LATENCY"
    RAND_LATENCY = "RAND_LATENCY"


class Label(Enum):
    DRIVER_META = "Driver"
    CONTROLLER_META = "ControllerMeta"
    ASSSD_BENCHMARK = "ASSSDBenchMark"
    RECORD_VALUE = "RecordValue"

class Relationship(Enum):
    CONTROLLER_META = "CONTROLLER_META"
    ASSSD_BENCHMARK = "ASSSD_BENCHMARK"
    RECORD_VALUE = "RECORD_VALUE"

@strawberry.type(description="记录值的基本单位", )
class RecordValue:
    id: int
    type: RecordType
    name: RecordName
    value: int
    unit: RecordUnit
    created_at: Optional[int] = None

@strawberry.input
class RecordValueInput:
    type: RecordType
    name: RecordName
    value: int
    unit: RecordUnit
    created_at: Optional[int] = None


@strawberry.type
class ASSSDBenchMark:
    id: int
    name: str = strawberry.field(description="测评结果名称")
    description: Optional[str] = strawberry.field(description="测评结果描述")
    created_at: Optional[int] = None

    record_value : list[Union[RecordValue, None]] = strawberry.field(resolver=record_value_resolver, description="测评结果细节数据",)

@strawberry.type
class ControllerMeta:
    id: int
    model: Optional[str] = None
    vendor: Optional[str] = None
    description: Optional[str] = None
    created_at: Optional[int] = None
    createdAt: Optional[int] = strawberry.field(default=None, deprecation_reason="Use `created_at` instead.")

    driver_meta: list[DriverMeta] = strawberry.field(resolver=driver_meta_resolver)


@strawberry.type
class DriverMeta:
    id: int
    name: Optional[str] = None
    model: Optional[str] = None
    vendor: Optional[str] = None
    capacity: Optional[float] = None
    created_at: Optional[int] = None

    controller_meta: list[Union[ControllerMeta, None]] = strawberry.field(resolver=controller_meta_resolver, description="controller_meta",)
    as_ssd_benchmark: list[Union[ASSSDBenchMark, None]] = strawberry.field(resolver=asssd_benchmark_resolver, description="as_ssd_benchmark",)


@strawberry.type
class Query:
    driver_meta: list[Union[DriverMeta, None]] = strawberry.field(resolver=driver_meta_resolver)
    controller_meta: list[Union[ControllerMeta, None]] = strawberry.field(resolver=controller_meta_resolver)


@strawberry.type
class Mutation:
    @strawberry.mutation
    def create_driver_meta(self, info: Info, name: str, model: str, vendor: str, capacity: float) -> DriverMeta:
        def cypher_create_driver_meta(tx: neo4j.Transaction, name, model, vendor, capacity):
            results = tx.run(
                f"""
                CREATE (m:`{Label.DRIVER_META.value}` 
                {{name: $name, model: $model, vendor: $vendor, capacity: $capacity, created_at: timestamp()}})
                RETURN id(m) as id, m
                """,
                name=name, model=model, vendor=vendor, capacity=capacity
            )
            return list(results)

        c_contex: CustomContext = info.context

        with c_contex.driver.session(database="neo4j") as session:
            results = session.execute_write(cypher_create_driver_meta, name=name, model=model, vendor=vendor, capacity=capacity)
            resolver_result = DriverMeta(id=results[0]["id"], **results[0]['m'])
            return resolver_result

    @strawberry.mutation
    def create_controller_meta(self, driver_id: int, info: Info, model: str, vendor: str, description: str) -> ControllerMeta:
        def cypher_create_controller_meta(tx: neo4j.Transaction, model, vendor, description):
            if driver_id:
                results = tx.run(
                    f"""
                    MATCH (n:`{Label.DRIVER_META.value}`) 
                    WHERE id(n) = $driver_id
                    CREATE (n)-[:{Relationship.CONTROLLER_META.value}]->(m:`{Label.CONTROLLER_META.value}` 
                    {{model: $model, vendor: $vendor, description: $description, created_at: timestamp()}})
                    RETURN id(m) as id, m
                    """,
                    driver_id=driver_id, model=model, vendor=vendor, description=description
                )
                return list(results)
            else:
                results = tx.run(
                    f"""
                    CREATE (m:`{Label.CONTROLLER_META.value}` {{model: $model, vendor: $vendor, description: $description}})
                    RETURN id(m) as id, m
                    """,
                    model=model, vendor=vendor, description=description
                )
                return list(results)

        c_contex: CustomContext = info.context

        with c_contex.driver.session(database="neo4j") as session:
            results = session.execute_write(cypher_create_controller_meta, model=model, vendor=vendor, description=description)
            resolver_result = ControllerMeta(id=results[0]["id"], **results[0]['m'])
            return resolver_result

    @strawberry.mutation
    def create_as_ssd_benchmark(self, info: Info, driver_id: int, name: str, description: str, records: list[RecordValueInput]) -> ASSSDBenchMark:
        def cypher_create_as_ssd_benchmark(tx: neo4j.Transaction, name, description, records: list[RecordValueInput]):
            records_decoded = [r.__dict__ for r in records]
            records_decoded = [dict((k, v.value) if isinstance(v, Enum) else (k, v) for k, v in r.items()) for r in records_decoded]
            table = tx.run(
                f"""
                MERGE (m:`{Label.ASSSD_BENCHMARK.value}` {{name: $name}})
                    ON CREATE SET m.description = $description, m.created_at = timestamp()
                    ON MATCH SET m.description = $description
                RETURN id(m) as id, m
                """,
                name=name, description=description,
            )
            table_result = list(table)
            record_id = table_result[0]["id"]
            for r in records_decoded:
                result = tx.run(
                    f"""
                    MERGE (m:`{Label.RECORD_VALUE.value}` {{type: $record.type, name: $record.name}})
                        ON CREATE SET m.created_at = timestamp(), m.value = $record.value, m.unit = $record.unit, m.name = $record.name
                        ON MATCH SET 
                            m.created_at = timestamp(), m.value = $record.value, m.unit = $record.unit, m.name = $record.name
                    return id(m) as id, m
                    """,
                    record = r
                )
                tx.run(
                    f"""
                    MATCH (n:`{Label.ASSSD_BENCHMARK.value}`), (m:`{Label.RECORD_VALUE.value}`)
                    WHERE id(n) = $record_id AND id(m) = $record_value_id
                    MERGE (n)-[:{Relationship.RECORD_VALUE.value}]->(m)
                    """,
                    record_id=record_id, record_value_id=result.single()['id']
                )
            tx.run(
                f"""
                MATCH (n:`{Label.DRIVER_META.value}`), (m:`{Label.ASSSD_BENCHMARK.value}`)
                WHERE id(n) = $driver_id AND id(m) = $record_id
                MERGE (n)-[:{Relationship.ASSSD_BENCHMARK.value}]->(m)
                """,
                driver_id=driver_id, record_id=record_id
            )
            return table_result
        
        c_contex: CustomContext = info.context

        with c_contex.driver.session(database="neo4j") as session:
            results = session.execute_write(cypher_create_as_ssd_benchmark, name=name, description=description, records=records)
            resolver_result = ASSSDBenchMark(id=results[0]["id"], **results[0]['m'])
            return resolver_result



async def get_context(custom_context=Depends(custom_context_dependency),):
    return custom_context

schema = strawberry.Schema(query=Query, mutation=Mutation)

graphql_app = GraphQLRouter(
    schema,
    context_getter=get_context,
)

