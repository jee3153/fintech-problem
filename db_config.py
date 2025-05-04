from sqlmodel import create_engine, SQLModel

def start_db_engine(url="postgresql://postgres:password@localhost:5432/postgres"):
    engine = create_engine(url)
    print("Creating database tables..")
    SQLModel.metadata.create_all(engine)
    print("Tables created successfully")
    return engine