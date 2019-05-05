from sqlalchemy import Column, BIGINT,DateTime,TEXT
from ckanext.datastore.backend.postgres import get_write_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker


Base=declarative_base()

class Query(Base):
    __tablename__ = "query"
    id = Column(BIGINT, primary_key=True)
    exec_timestamp = Column(DateTime)
    query = Column(TEXT)
    resource_id = Column(TEXT)
    query_hash = Column(TEXT)
    resultset_checksum = Column(TEXT)

engine=get_write_engine()
Session=sessionmaker(bind=engine)
session=Session()
Base.metadata.create_all(bind=engine)



class QueryStoreException(Exception):
    pass



class QueryStore:

    def __init__(self):
        self.engine = engine


    def store_query(self, exec_timestamp, query, query_hash, resultset_checksum,resource_id):
        q = session.query(Query).filter(Query.query == query,
                                        Query.query_hash == query_hash).first()

        if q:
            return q.id
        else:
            q = Query()
            q.query_hash = query_hash
            q.exec_timestamp = exec_timestamp
            q.query = query
            q.resultset_checksum = resultset_checksum
            q.resource_id=resource_id

            session.add(q)
            session.commit()
            return q.id


    def retrieve_query(self, pid):
        result=session.query(Query).filter(Query.id == pid).first()
        return result

    def retrive_last_entry(self):
        result = session.query(Query).order_by(Query.id.desc()).first()
        return result